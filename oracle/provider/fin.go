package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                   Provider = (*FinProvider)(nil)
	finDefaultEndpoints          = Endpoint{
		Name:         ProviderFin,
		Urls:         []string{"https://api.kujira.app"},
		PollInterval: 3 * time.Second,
	}
)

type (
	// FinProvider defines an oracle provider implemented by the FIN
	// public API.
	//
	// REF: https://docs.kujira.app/dapps-and-infrastructure/fin/coingecko-api
	FinProvider struct {
		provider
	}

	FinTickersResponse struct {
		Tickers []FinTicker `json:"tickers"`
	}

	FinTicker struct {
		Price       string `json:"last_price"`      // ex.: "2.0690000418"
		BaseVolume  string `json:"base_volume"`     // ex.: "4875.4890980000"
		QuoteVolume string `json:"target_volume"`   // ex.: "4875.4890980000"
		Base        string `json:"base_currency"`   // ex.: "LUNA"
		Quote       string `json:"target_currency"` // ex.: "USDC"
		Symbol      string `json:"ticker_id"`       // ex.: "LUNA_USDC"
	}
)

func NewFinProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*FinProvider, error) {
	provider := &FinProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToFinSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *FinProvider) getTickers() (FinTickersResponse, error) {
	content, err := p.httpGet("/api/coingecko/tickers")
	if err != nil {
		return FinTickersResponse{}, err
	}

	var tickersResponse FinTickersResponse
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return FinTickersResponse{}, err
	}

	return tickersResponse, nil
}

func (p *FinProvider) Poll() error {
	tickersResponse, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, ticker := range tickersResponse.Tickers {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.BaseVolume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *FinProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickersResponse, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickersResponse.Tickers {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToFinSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"USDT": "axlUSDT",
		"DYM":  "ADYM",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	return base + "_" + quote
}
