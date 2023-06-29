package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                        Provider = (*BitstampProvider)(nil)
	bitstampDefaultEndpoints          = Endpoint{
		Name:         ProviderBitstamp,
		Urls:         []string{"https://www.bitstamp.net"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// BitstampProvider defines an oracle provider implemented by the Bitstamp
	// public API.
	//
	// REF: https://www.bitstamp.net/api
	BitstampProvider struct {
		provider
	}

	BitstampTicker struct {
		Symbol string `json:"pair"`   // Symbol ex.: BTC/USD
		Price  string `json:"last"`   // Last price ex.: 30804
		Volume string `json:"volume"` // Total traded base asset volume ex.: 4594.62902316
	}
)

func NewBitstampProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitstampProvider, error) {
	provider := &BitstampProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBitstampSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BitstampProvider) getTickers() ([]BitstampTicker, error) {
	content, err := p.httpGet("/api/v2/ticker/")
	if err != nil {
		return nil, err
	}

	var tickers []BitstampTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *BitstampProvider) Poll() error {
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

func (p *BitstampProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, nil
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToBitstampSymbol(pair types.CurrencyPair) string {
	return pair.Join("/")
}
