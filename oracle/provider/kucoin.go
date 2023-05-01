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
	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *KucoinProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.Join("-")] = pair.String()
	}

	content, err := p.httpGet("/api/v1/market/allTickers")
	if err != nil {
		return err
	}

	var tickers KucoinTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers.Data.Ticker {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}
		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   now,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
