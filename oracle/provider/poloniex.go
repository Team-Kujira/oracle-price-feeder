package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                        Provider = (*PoloniexProvider)(nil)
	poloniexDefaultEndpoints          = Endpoint{
		Name:         ProviderPoloniex,
		Urls:         []string{"https://api.poloniex.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// PoloniexProvider defines an oracle provider implemented by the Poloniex
	// public API.
	//
	// REF: https://docs.poloniex.com
	PoloniexProvider struct {
		provider
	}

	PoloniexTicker struct {
		Symbol string `json:"symbol"`    // ec.: "BTC_USDT"
		Price  string `json:"close"`     // ex.: "23114.84"
		Volume string `json:"quantity"`  // ex.: "118.065209"
		Time   int64  `json:"closeTime"` // ex.: 1675862101027
	}
)

func NewPoloniexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PoloniexProvider, error) {
	provider := &PoloniexProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToPoloniexSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *PoloniexProvider) getTickers() ([]PoloniexTicker, error) {
	content, err := p.httpGet("/markets/ticker24h")
	if err != nil {
		return nil, err
	}

	var tickers []PoloniexTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *PoloniexProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			time.UnixMilli(ticker.Time),
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *PoloniexProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToPoloniexSymbol(pair types.CurrencyPair) string {
	return pair.Join("_")
}
