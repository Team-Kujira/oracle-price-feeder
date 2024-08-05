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
	_                     Provider = (*BingxProvider)(nil)
	bingxDefaultEndpoints          = Endpoint{
		Name:         ProviderBingx,
		Urls:         []string{"https://open-api.bingx.com"},
		PollInterval: 5 * time.Second,
	}
)

type (
	// BingxProvider defines an oracle provider implemented by the Bingx
	// public API.
	//
	// REF: https://bingx-api.github.io/docs/#/en-us/spot/changelog
	BingxProvider struct {
		provider
	}

	BingxTicker struct {
		Symbol string  `json:"symbol"`    // Symbol ex.: BTC-USDT
		Price  float64 `json:"lastPrice"` // Last price ex.: 0.0025
		Volume float64 `json:"volume"`    // Total traded base asset volume ex.: 1000
	}
)

func NewBingxProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BingxProvider, error) {
	provider := &BingxProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBingxSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BingxProvider) getTickers() ([]BingxTicker, error) {
	timestamp := time.Now().Unix()
	path := fmt.Sprintf("/openApi/spot/v1/ticker/24hr?timestamp=%d", timestamp)

	content, err := p.httpGet(path)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data []BingxTicker `json:"data"`
	}
	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

func (p *BingxProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	now := time.Now()

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
			now,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BingxProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToBingxSymbol(pair types.CurrencyPair) string {
	return pair.Join("-")
}
