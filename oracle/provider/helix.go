package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                     Provider = (*HelixProvider)(nil)
	helixDefaultEndpoints          = Endpoint{
		Name:         ProviderHelix,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
)

type (
	// Helix defines an oracle provider that uses Injective API nodes
	// to directly retrieve the on chain prices
	HelixProvider struct {
		provider
		contracts map[string]string
		token     map[string]HelixToken
	}

	HelixMarketsResponse struct {
		Markets []HelixMarket `json:"markets"`
	}

	HelixMarket struct {
		Market struct {
			Ticker     string `json:"ticker"`
			BaseDenom  string `json:"base_denom"`
			QuoteDenom string `json:"quote_denom"`
		} `json:"market"`
		MidPriceAndTob struct {
			Price string `json:"mid_price"`
		} `json:"mid_price_and_tob"`
	}

	HelixToken struct {
		Address  string `json:"address"`
		Decimals uint64 `json:"decimals"`
		Symbol   string `json:"symbol"`
		Name     string `json:"name"`
		Denom    string `json:"denom"`
	}
)

func NewHelixProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	helixTokenFile string,
	pairs ...types.CurrencyPair,
) (*HelixProvider, error) {
	provider := &HelixProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	res, err := http.Get(helixTokenFile)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err.Error())
	}

	var response []HelixToken
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	tokens := map[string]HelixToken{}
	for _, helixToken := range response {
		tokens[strings.ToUpper(helixToken.Denom)] = helixToken
	}
	provider.token = tokens

	provider.contracts = provider.endpoints.ContractAddresses

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToHelixSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *HelixProvider) Poll() error {
	timestamp := time.Now()

	markets, err := p.GetMarkets()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, market := range markets {
		if !p.isPair(market.Market.Ticker) {
			continue
		}

		pair, found := p.getPair(market.Market.Ticker)
		if !found {
			continue
		}

		rawPrice := strToDec(market.MidPriceAndTob.Price)
		if rawPrice.IsNil() {
			p.logger.Info().Str("ticker", market.Market.Ticker).Msg("No Price available from Helix")
			continue
		}
		baseToken, found := p.token[strings.ToUpper(market.Market.BaseDenom)]
		if !found {
			p.logger.Warn().Str("token", pair.Base).Str("denom", market.Market.BaseDenom).Msg("token not found in helix Token list")
			continue
		}
		quoteToken, found := p.token[strings.ToUpper(market.Market.QuoteDenom)]
		if !found {
			p.logger.Warn().Str("token", pair.Quote).Str("denom", market.Market.QuoteDenom).Msg("token not found in helix Token list")
			continue
		}
		var decimalDifference uint64
		var multiplier sdk.Dec
		if quoteToken.Decimals > baseToken.Decimals {
			decimalDifference = quoteToken.Decimals - baseToken.Decimals
			multiplier = sdk.NewDec(10).Power(decimalDifference)
			rawPrice = rawPrice.Mul(invertDec(multiplier))
		} else {
			decimalDifference = baseToken.Decimals - quoteToken.Decimals
			multiplier = sdk.NewDec(10).Power(decimalDifference)
			rawPrice = rawPrice.Mul(multiplier)
		}

		p.logger.Warn().Uint64("decimalDifference", decimalDifference).Str("ticker", market.Market.Ticker).
			Msg("decimalDifference")

		p.setTickerPrice(
			market.Market.Ticker,
			rawPrice,
			sdk.OneDec(), // helix doesn't appear to report volume
			timestamp,
		)
	}

	return nil
}

func (p *HelixProvider) GetMarkets() ([]HelixMarket, error) {
	path := "/injective/exchange/v1beta1/spot/full_markets?status=Active&with_mid_price_and_tob=true"

	content, err := p.httpGet(path)
	if err != nil {
		return nil, err
	}

	var response HelixMarketsResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, err
	}

	var markets []HelixMarket
	for _, market := range response.Markets {
		market.Market.Ticker = strings.ToUpper(market.Market.Ticker)
		markets = append(markets, market)
	}

	return markets, nil
}

func (p *HelixProvider) GetAvailablePairs() (map[string]struct{}, error) {
	markets, err := p.GetMarkets()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, market := range markets {
		symbols[market.Market.Ticker] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToHelixSymbol(pair types.CurrencyPair) string {
	return pair.Join("/")
}
