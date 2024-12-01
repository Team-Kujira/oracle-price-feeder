package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                     Provider = (*BybitProvider)(nil)
	bybitDefaultEndpoints          = Endpoint{
		Name:         ProviderBybit,
		Urls:         []string{"https://api.bybit.com", "https://api.bytick.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// BybitProvider defines an oracle provider implemented by the ByBit
	// public API.
	//
	// REF: https://docs.kujira.app/dapps-and-infrastructure/fin/coingecko-api
	BybitProvider struct {
		provider
	}

	BybitTickersResponse struct {
		Result BybitTickersResult `json:"result"`
	}

	BybitTickersResult struct {
		List []BybitTicker `json:"list"`
	}

	BybitTicker struct {
		Symbol string `json:"symbol"`    // ex.: "LUNAUSDT"
		Price  string `json:"lastPrice"` // ex.: "21127.86"
		Volume string `json:"volume24h"` // ex.: "211.378621"
	}
)

func NewBybitProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BybitProvider, error) {
	provider := &BybitProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBybitSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BybitProvider) getTickers() (BybitTickersResponse, error) {
	content, err := p.httpGet("/v5/market/tickers?category=spot")
	if err != nil {
		return BybitTickersResponse{}, err
	}

	var tickersResponse BybitTickersResponse
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return BybitTickersResponse{}, err
	}

	return tickersResponse, nil
}

func (p *BybitProvider) Poll() error {
	tickersResponse, err := p.getTickers()
	if err != nil {
		return err
	}

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickersResponse.Result.List {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BybitProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Result.List {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToBybitSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"MATIC": "POL",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	return base + quote
}
