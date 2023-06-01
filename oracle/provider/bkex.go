package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                    Provider = (*BkexProvider)(nil)
	bkexDefaultEndpoints          = Endpoint{
		Name:         ProviderBkex,
		Urls:         []string{"https://api.bkex.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// BkexProvider defines an oracle provider implemented by the BKEX
	// public API.
	//
	// REF: https://bkexapi.github.io/docs/api_en.htm
	BkexProvider struct {
		provider
	}

	BkexTickersResponse struct {
		Data []BkexTicker `json:"data"`
	}

	BkexTicker struct {
		Symbol string  `json:"symbol"` // ex.: "BTC_USDT"
		Price  float64 `json:"close"`  // ex.: 23197.24
		Volume float64 `json:"volume"` // ex.: 17603.2275
		Time   int64   `json:"ts"`     // ex.: 1675858514163
	}
)

func NewBkexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BkexProvider, error) {
	provider := &BkexProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBkexSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BkexProvider) getTickers() (BkexTickersResponse, error) {
	content, err := p.httpGet("/v2/q/tickers")
	if err != nil {
		return BkexTickersResponse{}, err
	}

	var tickers BkexTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return BkexTickersResponse{}, err
	}

	return tickers, nil
}

func (p *BkexProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers.Data {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			floatToDec(ticker.Price),
			floatToDec(ticker.Volume),
			time.UnixMilli(ticker.Time),
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BkexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Data {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToBkexSymbol(pair types.CurrencyPair) string {
	return pair.Join("_")
}
