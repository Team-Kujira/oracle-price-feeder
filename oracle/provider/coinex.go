package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*CoinexProvider)(nil)
	coinexDefaultEndpoints          = Endpoint{
		Name:         ProviderCoinex,
		Urls:         []string{"https://api.coinex.com/v1"},
		PollInterval: 3 * time.Second,
	}
)

type (
	// CoinexProvider defines an oracle provider implemented by the Coinex
	// public API.
	//
	// REF: https://viabtc.github.io/coinex_api_en_doc/spot
	CoinexProvider struct {
		provider
	}

	CoinexTickersResponse struct {
		Data struct {
			Tickers map[string]CoinexTicker `json:"ticker"`
		} `json:"data"`
	}

	CoinexTicker struct {
		Price  string `json:"last"` // ex.: "0.2322211"
		Volume string `json:"vol"`  // ex.: "2.01430624"
	}
)

func NewCoinexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CoinexProvider, error) {
	provider := &CoinexProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *CoinexProvider) getTickers() (map[string]CoinexTicker, error) {
	content, err := p.httpGet("/market/ticker/all")
	if err != nil {
		return nil, err
	}

	var response CoinexTickersResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, err
	}

	return response.Data.Tickers, nil
}

func (p *CoinexProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for symbol, ticker := range tickers {
		if !p.isPair(symbol) {
			continue
		}

		p.setTickerPrice(
			symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *CoinexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for symbol := range tickers {
		symbols[symbol] = struct{}{}
	}

	return symbols, nil
}
