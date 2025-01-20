package provider

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                       Provider = (*BinanceProvider)(nil)
	binanceDefaultEndpoints          = Endpoint{
		Name:         ProviderBinance,
		Urls:         []string{"https://api.binance.com"},
		PollInterval: 6 * time.Second,
	}
	binanceUSDefaultEndpoints = Endpoint{
		Name:         ProviderBinanceUS,
		Urls:         []string{"https://api.binance.us"},
		PollInterval: 6 * time.Second,
	}
)

type (
	// BinanceProvider defines an Oracle provider implemented by the Binance public
	// API.
	//
	// REF: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
	// REF: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
	BinanceProvider struct {
		provider
	}

	BinanceTicker struct {
		Symbol    string `json:"symbol"`    // Symbol ex.: BTCUSDT
		LastPrice string `json:"lastPrice"` // Last price ex.: 0.0025
		Volume    string `json:"volume"`    // Total traded base asset volume ex.: 20
	}
)

func NewBinanceProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BinanceProvider, error) {
	provider := &BinanceProvider{}

	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	if endpoints.Name == ProviderBinance {
		// Add some failover URLs in random order for Binance global,
		// avoid using the same URL at the same time on every feeder
		urls := []string{
			"https://api1.binance.com",
			"https://api2.binance.com",
			"https://api3.binance.com",
			"https://api4.binance.com",
		}

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(
			len(urls),
			func(i, j int) { urls[i], urls[j] = urls[j], urls[i] },
		)

		provider.endpoints.Urls = append(provider.endpoints.Urls, urls...)
	}

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBinanceSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BinanceProvider) getTickers() ([]BinanceTicker, error) {
	content, err := p.httpGet("/api/v3/ticker/24hr")
	if err != nil {
		return []BinanceTicker{}, err
	}

	var tickers []BinanceTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return []BinanceTicker{}, err
	}

	return tickers, nil
}

func (p *BinanceProvider) Poll() error {
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
			strToDec(ticker.LastPrice),
			strToDec(ticker.Volume),
			now,
		)
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BinanceProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToBinanceSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
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

	return base + quote
}
