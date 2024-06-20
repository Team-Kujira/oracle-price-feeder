package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*PionexProvider)(nil)
	pionexDefaultEndpoints          = Endpoint{
		Name:         ProviderPionex,
		Urls:         []string{"https://api.pionex.com"},
		PollInterval: 3 * time.Second,
	}
)

type (
	// PionexProvider defines an oracle provider implemented by the Pionex
	// public API.
	//
	// REF: https://pionex-doc.gitbook.io/apidocs
	PionexProvider struct {
		provider
	}

	PionexTicker struct {
		Symbol string `json:"symbol"` // Symbol ex.: BTC_USDT
		Price  string `json:"close"`  // Last price ex.: 0.0025
		Volume string `json:"volume"` // Total traded base asset volume ex.: 1000
	}
)

func NewPionexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PionexProvider, error) {
	provider := &PionexProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToPionexSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *PionexProvider) getTickers() ([]PionexTicker, error) {
	content, err := p.httpGet("/api/v1/market/tickers")
	if err != nil {
		return nil, err
	}

	var response struct {
		Data struct {
			Tickers []PionexTicker `json:"tickers"`
		} `json:"data"`
	}
	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, err
	}

	return response.Data.Tickers, nil
}

func (p *PionexProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			now,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *PionexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToPionexSymbol(pair types.CurrencyPair) string {
	return pair.Join("_")
}
