package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                        Provider = (*CoinbaseProvider)(nil)
	coinbaseDefaultEndpoints          = Endpoint{
		Name: ProviderCoinbase,
		Urls: []string{"https://api.exchange.coinbase.com"},
	}
)

type (
	// CoinbaseProvider defines an oracle provider implemented by the XT.COM
	// public API.
	//
	// REF: https://bitgetlimited.github.io/apidoc/en/spot
	CoinbaseProvider struct {
		provider
	}

	CoinbaseTicker struct {
		Price  string `json:"price"`  // ex.: "24014.11"
		Volume string `json:"volume"` // ex.: "7421.5009"
		Time   string `json:"time"`   // ex.: "1660704288118"
	}

	CoinbaseTradingPair struct {
		Symbol string `json:"id"` // ex.: "ADA-BTC"
	}
)

func NewCoinbaseProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CoinbaseProvider, error) {
	provider := &CoinbaseProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToCoinbaseSymbol)

	interval := time.Duration(len(provider.getAllPairs())/10*2+1) * time.Second

	go startPolling(provider, interval, logger)
	return provider, nil
}

func (p *CoinbaseProvider) Poll() error {
	i := 0
	for symbol, pair := range p.getAllPairs() {
		go func(p *CoinbaseProvider, symbol string, pair types.CurrencyPair) {
			path := fmt.Sprintf("/products/%s/ticker", symbol)
			content, err := p.httpGet(path)
			if err != nil {
				return
			}

			var ticker CoinbaseTicker
			err = json.Unmarshal(content, &ticker)
			if err != nil {
				return
			}

			now := time.Now()

			p.mtx.Lock()
			defer p.mtx.Unlock()

			p.setTickerPrice(
				symbol,
				strToDec(ticker.Price),
				strToDec(ticker.Volume),
				now,
			)
		}(p, symbol, pair)
		// Coinbase has a rate limit of 10req/s, sleeping 1.2s before running
		// the next batch of requests
		i = i + 1
		if i == 10 {
			i = 0
			time.Sleep(time.Millisecond * 1200)
		}
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *CoinbaseProvider) GetAvailablePairs() (map[string]struct{}, error) {
	content, err := p.httpGet("/products")
	if err != nil {
		return nil, err
	}

	var pairs []CoinbaseTradingPair
	err = json.Unmarshal(content, &pairs)
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, pair := range pairs {
		symbols[pair.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToCoinbaseSymbol(pair types.CurrencyPair) string {
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

	return base + "-" + quote
}
