package provider

import (
	"context"
	"encoding/json"
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
	}

	HelixMarketsResponse struct {
		Markets []HelixMarket `json:"markets"`
	}

	HelixMarket struct {
		Market struct {
			Ticker string `json:"ticker"`
		} `json:"market"`
		MidPriceAndTob struct {
			Price string `json:"mid_price"`
		} `json:"mid_price_and_tob"`
	}
)

func NewHelixProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
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

		p.setTickerPrice(
			market.Market.Ticker,
			strToDec(market.MidPriceAndTob.Price),
			sdk.ZeroDec(),
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

	return response.Markets, nil
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
