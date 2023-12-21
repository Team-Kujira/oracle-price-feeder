package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                    Provider = (*MexcProvider)(nil)
	mexcDefaultEndpoints          = Endpoint{
		Name:         ProviderMexc,
		Urls:         []string{"https://api.mexc.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// MexcProvider defines an oracle provider implemented by the Kucoin
	// public API.
	//
	// REF: https://mxcdevelop.github.io/apidocs/spot_v3_en
	MexcProvider struct {
		provider
	}

	MexcTicker struct {
		Symbol string `json:"symbol"`    // Symbol ex.: BTC-USDT
		Price  string `json:"lastPrice"` // Last price ex.: 0.0025
		Volume string `json:"volume"`    // Total traded base asset volume ex.: 1000
	}
)

func NewMexcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*MexcProvider, error) {
	provider := &MexcProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToMexcSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *MexcProvider) getTickers() ([]MexcTicker, error) {
	content, err := p.httpGet("/api/v3/ticker/24hr")
	if err != nil {
		return nil, err
	}

	var tickers []MexcTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *MexcProvider) Poll() error {
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

func (p *MexcProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToMexcSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"AXL": "WAXL",
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
