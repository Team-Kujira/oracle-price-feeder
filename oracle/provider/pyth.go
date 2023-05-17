package provider

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_ Provider = (*PythProvider)(nil)

	pythDefaultEndpoints = Endpoint{
		Name:         ProviderPyth,
		Urls:         []string{"https://xc-mainnet.pyth.network"},
		PollInterval: 6 * time.Second,
	}
)

type (
	// Pyth defines an oracle provider that uses the Pyth price service
	// https://docs.pyth.network/pythnet-price-feeds/price-service
	PythProvider struct {
		provider
	}

	PythTicker struct {
		ID    string    `json:"id"`
		Price PythPrice `json:"price"`
	}

	PythPrice struct {
		Price    string `json:"price"`
		Exponent int64  `json:"expo"`
		Time     int64  `json:"publish_time"`
	}
)

func NewPythProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PythProvider, error) {
	provider := &PythProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToPythSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *PythProvider) Poll() error {
	stubs := []string{}
	for symbol := range p.getAllPairs() {
		stubs = append(stubs, "ids="+symbol)
	}

	// The API expects at least 2 ids
	if len(stubs) == 1 {
		stubs = append(stubs, stubs[0])
	}

	path := "/api/latest_price_feeds?" + strings.Join(stubs, "&")
	content, err := p.httpGet(path)
	if err != nil {
		return err
	}

	var tickers []PythTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	pairs := p.getAllPairs()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers {
		_, found := pairs[ticker.ID]
		if !found {
			continue
		}

		exponent := ticker.Price.Exponent
		factor := sdk.NewDec(10)

		if exponent < 0 {
			factor = sdk.NewDec(1).Quo(factor.Power(uint64(exponent * -1)))
		} else {
			factor = factor.Power(uint64(exponent))
		}

		price := strToDec(ticker.Price.Price).Mul(factor)

		// Use now for testing, as only crypto is available 24/7
		// timestamp := time.Unix(ticker.Price.Time, 0)
		timestamp := time.Now()

		p.setTickerPrice(
			ticker.ID,
			price,
			sdk.NewDec(1),
			timestamp,
		)
	}

	p.logger.Debug().Msg("updated USK")
	return nil
}

func (p *PythProvider) GetAvailablePairs() (map[string]struct{}, error) {
	content, err := p.httpGet("/api/price_feed_ids")
	if err != nil {
		return nil, err
	}

	var ids []string
	err = json.Unmarshal(content, &ids)
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, id := range ids {
		symbols[id] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToPythSymbol(pair types.CurrencyPair) string {
	// https://pyth.network/developers/price-feed-ids#pyth-evm-mainnet
	mapping := map[string]string{
		"AUDUSD":  "67a6f93030420c1c9e3fe37c1ab6b77966af82f995944a9fefce357a22854a80",
		"EURUSD":  "a995d00bb36a63cef7fd2c287dc105fc8f3d93779f062f09551b0af3e81ec30b",
		"GBPUSD":  "84c2dde9633d93d1bcad84e7dc41c9d56578b7ec52fabedc1f335d673df0a7c1",
		"USDJPY":  "ef2c98c804ba503c6a707e38be4dfbb16683775f195b091252bf24693042fd52",
		"AAPLUSD": "49f6b65cb1de6b10eaf75e7c03ca029c306d0357e91b5311b175084a5ad55688",
		"NFLXUSD": "8376cfd7ca8bcdf372ced05307b24dced1f15b1afafdeff715664598f15a3dd2",
		"TSLAUSD": "16dad506d7db8da01c87581c87ca897a012a153557d4d578c3b9c9e1bc0632f1",
	}

	symbol, found := mapping[pair.String()]
	if !found {
		return ""
	}
	return symbol
}
