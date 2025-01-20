package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                    Provider = (*GateProvider)(nil)
	gateDefaultEndpoints          = Endpoint{
		Name:         ProviderGate,
		Urls:         []string{"https://api.gateio.ws"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// GateProvider defines an oracle provider implemented by the Gate.io
	// public API.
	//
	// REF: https://www.gate.io/docs/developers/apiv4/en/
	GateProvider struct {
		provider
	}

	GateTicker struct {
		Symbol string `json:"currency_pair"` // Symbol ex.: BTC_USDT
		Price  string `json:"last"`          // Last price ex.: 0.0025
		Volume string `json:"base_volume"`   // Total traded base asset volume ex.: 1000
	}
)

func NewGateProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*GateProvider, error) {
	provider := &GateProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToGateSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *GateProvider) getTickers() ([]GateTicker, error) {
	content, err := p.httpGet("/api/v4/spot/tickers")
	if err != nil {
		return nil, err
	}

	var tickers []GateTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *GateProvider) Poll() error {
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

func (p *GateProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToGateSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"AXL":   "WAXL",
		"MATIC": "POL",
		"FTM":   "S",
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
