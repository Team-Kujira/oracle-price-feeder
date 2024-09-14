package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*KucoinProvider)(nil)
	kucoinDefaultEndpoints          = Endpoint{
		Name:         ProviderKucoin,
		Urls:         []string{"https://api.kucoin.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// KucoinProvider defines an oracle provider implemented by the Kucoin
	// public API.
	//
	// REF: https://docs.kucoin.com/?lang=en_US
	KucoinProvider struct {
		provider
	}

	KucoinTickersResponse struct {
		Code string                    `json:"code"`
		Data KucoinTickersResponseData `json:"data"`
	}

	KucoinTickersResponseData struct {
		Ticker []KucoinTicker `json:"ticker"`
	}

	KucoinTicker struct {
		Symbol string `json:"symbol"` // Symbol ex.: BTC-USDT
		Price  string `json:"last"`   // Last price ex.: 0.0025
		Volume string `json:"vol"`    // Total traded base asset volume ex.: 1000
	}
)

func NewKucoinProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*KucoinProvider, error) {
	provider := &KucoinProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToKucoinSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *KucoinProvider) getTickers() (KucoinTickersResponse, error) {
	content, err := p.httpGet("/api/v1/market/allTickers")
	if err != nil {
		return KucoinTickersResponse{}, err
	}

	var tickers KucoinTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return KucoinTickersResponse{}, err
	}

	return tickers, nil
}

func (p *KucoinProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()

	for _, ticker := range tickers.Data.Ticker {
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

func (p *KucoinProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Data.Ticker {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToKucoinSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"AXL":   "WAXL",
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

	return base + "-" + quote
}
