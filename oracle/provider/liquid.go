package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"price-feeder/config"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

const (
	liquidWSHost       = "blue.kujira.app"
	liquidWSPath       = ""
	liquidRestHost     = "blue.kujira.app"
	liquidRestPath     = ""
	liquidSupplyGrpc   = "https://lcd-stride.synergynodes.com"
	liquidSupplyDenom  = "/cosmos/bank/v1beta1/supply"
	liquidStakeGrpc    = "https://cosmoshub-lcd.stakely.io"
	liquidStakeAddress = "cosmos10uxaa5gkxpeungu2c9qswx035v6t3r24w6v2r6dxd858rq2mzknqj8ru28"
	liquidStakedPath   = "/cosmos/staking/v1beta1/delegations/"
	liquidLiquidPath   = "/cosmos/bank/v1beta1/balances/"
	// liquidRewardsPath  = "/cosmos/distribution/v1beta1/delegators/" + liquidStakeAddress + "/rewards"
)

var _ Provider = (*LiquidProvider)(nil)

type (
	// LiquidProvider defines an Oracle provider implemented by gRPC servers
	// Reference calc below - all staked assets and pending rewards are compared
	// against total supply of liquid staking tokens to arrive at a value of
	// liquid staking token vs. staked asset
	//
	// bc -l <<< "scale=9;( \
	// $(curl -s "https://cosmoshub-lcd.stakely.io/cosmos/staking/v1beta1/delegations/cosmos10uxaa5gkxpeungu2c9qswx035v6t3r24w6v2r6dxd858rq2mzknqj8ru28" | jq '[.delegation_responses[].balance.amount | tonumber] | add ')\
	// +$(curl -s "https://cosmoshub-lcd.stakely.io/cosmos/distribution/v1beta1/delegators/cosmos10uxaa5gkxpeungu2c9qswx035v6t3r24w6v2r6dxd858rq2mzknqj8ru28/rewards" | jq '[.rewards[].reward[].amount | tonumber] | add ')\
	// +$(curl -s "https://cosmoshub-lcd.stakely.io/cosmos/bank/v1beta1/balances/cosmos10uxaa5gkxpeungu2c9qswx035v6t3r24w6v2r6dxd858rq2mzknqj8ru28" | jq '.balances[].amount | tonumber'))\
	// /$(curl -s "https://lcd-stride.synergynodes.com/cosmos/bank/v1beta1/supply/stuatom" | jq '.amount.amount | tonumber')"
	//
	LiquidProvider struct {
		stakeURL        url.URL
		stakeLiquidURL  url.URL
		supplyURL       url.URL
		client          *http.Client
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       config.ProviderEndpoint
		tickers         map[string]LiquidTicker       // Symbol => LiquidTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// LiquidTicker ticker price response. https://pkg.go.dev/encoding/json#Unmarshal
	// Unmarshal matches incoming object keys to the keys used by Marshal (either the
	// struct field name or its tag), preferring an exact match but also accepting a
	// case-insensitive match. C field which is Statistics close time is not used, but
	// it avoids to implement specific UnmarshalJSON.

	LiquidTicker struct {
		Symbol    string `json:"symbol"` // Symbol ex.: STATOM_ATOM
		LastPrice string `json:"p"`      // Last price ex.: 0.0025
		Volume    string `json:"v"`      // Total traded base asset volume ex.: 1000
		C         uint64 `json:"C"`      // Statistics close time
	}

	LiquidTickerData struct {
		LastPrice float64 `json:"p"` // Last price ex.: 0.0025
		Volume    float64 `json:"v"` // Total traded base asset volume ex.: 1000
	}

	LiquidTickerResult struct {
		Channel string                      `json:"channel"` // expect "push.overview"
		Symbol  map[string]LiquidTickerData `json:"data"`    // this key is the Symbol ex.: ATOM_USDT
	}

	// LiquidStake
	LiquidStake struct {
		Responses []LiquidStakeDetail `json:"delegation_responses"` // expect "push.kline"
	}

	// LiquidStakeDetail
	LiquidStakeDetail struct {
		Delegation []LiquidStakeShares  `json:"delegation"` // expect "push.kline"
		Balance    []LiquidLiquidDetail `json:"balance"`    // expect "push.kline"
	}

	// LiquidLiquid
	LiquidLiquid struct {
		Data []LiquidLiquidDetail `json:"balances"` // expect "push.kline"
	}

	// LiquidLiquidDetail
	LiquidLiquidDetail struct {
		ticker string  `json:"denom"`  // expect "uatom"
		amount float64 `json:"amount"` // expect "123456"
	}

	// LiquidStakeShares
	LiquidStakeShares struct {
		Delegator string  `json:"delegator_address"` // expect "cosmos10uxaa5gkxpeungu2c9qswx035v6t3r24w6v2r6dxd858rq2mzknqj8ru28"
		Validator string  `json:"validator_address"` // expect "cosmosvaloper1qwl879nx9t6kef4supyazayf7vjhennyh568ys"
		Shares    float64 `json:"shares"`            // expect "123.456"
	}

	// LiquidStakeShares
	LiquidSupply struct {
		Data []LiquidSupplyDetail `json:"supply"` // expect "array"
	}

	// LiquidStakeShares
	LiquidSupplyDetail struct {
		ticker string  `json:"denom"`  // expect "stuatom"
		amount float64 `json:"amount"` // expect "push.kline"
	}
)

func NewLiquidProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints config.ProviderEndpoint,
	pairs ...types.CurrencyPair,
) (*LiquidProvider, error) {
	if (endpoints.Name) != config.ProviderLiquid {
		endpoints = config.ProviderEndpoint{
			Name:         config.ProviderLiquid,
			Rest:         liquidRestHost,
			Websocket:    liquidWSHost,
			SupplyGrpc:   liquidSupplyGrpc,
			StakeGrpc:    liquidStakeGrpc,
			StakeAccount: liquidStakeAddress,
		}
	}

	stakeURL := url.URL{
		Scheme: "http",
		Host:   endpoints.StakeGrpc,
		Path:   liquidStakedPath + endpoints.StakeAccount,
	}

	stakeLiquidURL := url.URL{
		Scheme: "http",
		Host:   endpoints.StakeGrpc,
		Path:   liquidStakedPath + endpoints.StakeAccount,
	}

	supplyURL := url.URL{
		Scheme: "http",
		Host:   endpoints.SupplyGrpc,
		Path:   liquidWSPath,
	}

	provider := &LiquidProvider{
		stakeURL:        stakeURL,
		stakeLiquidURL:  stakeLiquidURL,
		supplyURL:       supplyURL,
		client:          newDefaultHTTPClient(),
		logger:          logger.With().Str("provider", "liquid").Logger(),
		endpoints:       endpoints,
		tickers:         map[string]LiquidTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	if err := provider.SubscribeCurrencyPairs(pairs...); err != nil {
		return nil, err
	}

	return provider, nil
}

// GetTickerPrices returns the tickerPrices based on the provided pairs.
func (p *LiquidProvider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]TickerPrice, error) {
	// Get stake
	stake := 0.0

	resp, err := http.Get(p.endpoints.StakeGrpc + liquidStakedPath + p.endpoints.StakeAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to make liquid request: %w", err)
	}
	defer resp.Body.Close()

	bz, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read liquid response body: %w", err)
	}

	var stakeResp LiquidStake
	if err := json.Unmarshal(bz, &stakeResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal liquid response body: %w", err)
	}

	for _, tr := range stakeResp.Responses {
		for _, d := range tr.Delegation {
			stake += d.Shares
		}
	}

	// Get liquid
	respLiq, err := http.Get(p.endpoints.StakeGrpc + liquidLiquidPath + p.endpoints.StakeAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to make liquid request: %w", err)
	}
	defer respLiq.Body.Close()

	bzLiq, err := ioutil.ReadAll(respLiq.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read liquid response body: %w", err)
	}

	var stakeLiqResp LiquidLiquid
	if err := json.Unmarshal(bzLiq, &stakeLiqResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal liquid response body: %w", err)
	}
	for _, tr := range stakeLiqResp.Data {
		stake += tr.amount
	}

	// Get supply
	respSupply, err := http.Get(p.endpoints.SupplyGrpc + liquidSupplyDenom)
	if err != nil {
		return nil, fmt.Errorf("failed to make liquid request: %w", err)
	}
	defer respSupply.Body.Close()

	bzSupply, err := ioutil.ReadAll(respSupply.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read liquid response body: %w", err)
	}

	var tokensSupplyResp LiquidSupply
	if err := json.Unmarshal(bzSupply, &tokensSupplyResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal liquid response body: %w", err)
	}
	// regular expression to reject all lowercase letters (e.g. st, stk)
	re := regexp.MustCompile(`[^a-z]+`)
	// regular expression to reject all uppercase letters (e.g. ATOM, STARS)
	re2 := regexp.MustCompile(`[^A-Z]+`)

	baseDenomIdx := make(map[string]types.CurrencyPair)
	for _, cp := range pairs {
		baseDenomIdx[strings.ToUpper(re2.FindString(cp.Base)+"u"+re.FindString(cp.Base))] = cp
	}

	tickerPrices := make(map[string]TickerPrice, len(pairs))
	for _, tr := range tokensSupplyResp.Data {

		symbol := strings.ToUpper(tr.ticker) // symbol == base in a currency pair

		cp, ok := baseDenomIdx[symbol]
		if !ok {
			// skip tokens that are not requested
			continue
		}

		if _, ok := tickerPrices[symbol]; ok {
			return nil, fmt.Errorf("duplicate token found in liquid response: %s", symbol)
		}

		supply := tr.amount

		// Divide stake by supply
		priceRaw := strconv.FormatFloat(stake/supply, 'f', -1, 64)
		price, err := sdk.NewDecFromStr(priceRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to read liquid price (%s) for %s", priceRaw, symbol)
		}

		volumeRaw := strconv.FormatFloat(tr.amount, 'f', -1, 64)
		volume, err := sdk.NewDecFromStr(volumeRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to read liquid volume (%s) for %s", volumeRaw, symbol)
		}

		// Return price
		tickerPrices[cp.String()] = TickerPrice{Price: price, Volume: volume}
	}

	for _, cp := range pairs {
		if _, ok := tickerPrices[cp.String()]; !ok {
			return nil, fmt.Errorf("missing exchange rate for %s", cp.String())
		}
	}

	return tickerPrices, nil
}

// GetCandlePrices returns the candlePrices based on the provided pairs.
// Returns nothing here
func (p *LiquidProvider) GetCandlePrices(pairs ...types.CurrencyPair) (map[string][]CandlePrice, error) {
	candlePrices := make(map[string][]CandlePrice, len(pairs))

	for _, cp := range pairs {
		key := cp.String()
		prices, err := p.getCandlePrices(key)
		if err != nil {
			return nil, err
		}
		candlePrices[key] = prices
	}

	return candlePrices, nil
}

// SubscribeCurrencyPairs performs a no-op since liquid does not use websockets
func (p *LiquidProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	if len(cps)-1 < 0 {
		return fmt.Errorf("currency pairs is empty")
	}
	return nil
}

// subscribedPairsToSlice returns the map of subscribed pairs as a slice.
func (p *LiquidProvider) subscribedPairsToSlice() []types.CurrencyPair {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	return types.MapPairsToSlice(p.subscribedPairs)
}

func (p *LiquidProvider) getCandlePrices(key string) ([]CandlePrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	return []CandlePrice{}, fmt.Errorf("failed to get candle prices for %s", key)
}

// setSubscribedPairs sets N currency pairs to the map of subscribed pairs.
func (p *LiquidProvider) setSubscribedPairs(cps ...types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, cp := range cps {
		p.subscribedPairs[cp.String()] = cp
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["stATOMATOM" => {}, "stkATOMATOM" => {}].
func (p *LiquidProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.SupplyGrpc + liquidSupplyDenom)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary LiquidSupply
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	// regular expression to reject all lowercase letters (e.g. st, stk)
	re := regexp.MustCompile(`[^a-z]+`)

	availablePairs := make(map[string]struct{}, len(pairsSummary.Data))
	for _, pair := range pairsSummary.Data {
		cp := types.CurrencyPair{
			Base:  strings.ToUpper(pair.ticker),
			Quote: strings.ToUpper(re.FindString(pair.ticker)),
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// currencyPairToLiquidPair receives a currency pair and return liquid
// ticker symbol atomusdt@ticker.
func currencyPairToLiquidPair(cp types.CurrencyPair) string {
	return strings.ToUpper(cp.Base + "_" + cp.Quote)
}
