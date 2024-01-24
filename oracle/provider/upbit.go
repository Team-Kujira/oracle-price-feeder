package provider

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                     Provider = (*UpbitProvider)(nil)
	upbitDefaultEndpoints          = Endpoint{
		Name:         ProviderUpbit,
		Urls:         []string{"https://api.upbit.com"},
		PollInterval: 5 * time.Second,
	}
)

type (
	// UpbitProvider defines an oracle provider implemented by the upbit.com
	// public API.
	//
	// REF: https://docs.upbit.com/reference
	UpbitProvider struct {
		provider
	}

	UpbitTicker struct {
		Time   int64   `json:"trade_timestamp"`     // Timestamp ex.: 1705842744041
		Price  float64 `json:"trade_price"`         // Last price ex.: 0.0025
		Volume float64 `json:"acc_trade_price_24h"` // Total traded base asset volume ex.: 1000
		Symbol string  `json:"market"`              // Symbol ex.: "BTC-ETH"
	}

	UpbitMarket struct {
		Symbol string `json:"market"` // Market name ex.: "KRW-BTC"
	}
)

func NewUpbitProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*UpbitProvider, error) {
	provider := &UpbitProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToUpbitSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *UpbitProvider) getTickers() ([]UpbitTicker, error) {
	markets := []string{}
	for symbol := range p.getAllPairs() {
		markets = append(markets, symbol)
	}

	path := "/v1/ticker?markets=" + strings.Join(markets, ",")

	content, err := p.httpGet(path)
	if err != nil {
		return nil, err
	}

	var tickers []UpbitTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *UpbitProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			floatToDec(ticker.Price),
			floatToDec(ticker.Volume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *UpbitProvider) GetAvailablePairs() (map[string]struct{}, error) {
	content, err := p.httpGet("/v1/market/all?isDetails=false")
	if err != nil {
		return nil, err
	}

	var markets []UpbitMarket

	err = json.Unmarshal(content, &markets)
	if err != nil {
		p.logger.Error().
			Err(err).
			Msg("failed getting markets")
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range markets {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToUpbitSymbol(pair types.CurrencyPair) string {
	// mapping := map[string]string{
	// 	"AXL": "WAXL",
	// }

	// base, found := mapping[pair.Base]
	// if !found {
	// 	base = pair.Base
	// }

	// quote, found := mapping[pair.Quote]
	// if !found {
	// 	quote = pair.Quote
	// }

	pair = pair.Swap()

	return pair.Join("-")
}
