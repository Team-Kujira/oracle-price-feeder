package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                       Provider = (*BitmartProvider)(nil)
	bitmartDefaultEndpoints          = Endpoint{
		Name:         ProviderBitmart,
		Rest:         "https://api-cloud.bitmart.com",
		PollInterval: 6 * time.Second,
	}
)

type (
	// BitmartProvider defines an oracle provider implemented by the BitMart
	// public API.
	//
	// REF: https://developer-pro.bitmart.com/en/spot
	BitmartProvider struct {
		provider
	}

	BitmartTickersResponse struct {
		Data BitmartTickersData `json:"data"`
	}

	BitmartTickersData struct {
		Tickers []BitmartTicker `json:"tickers"`
	}

	BitmartTicker struct {
		Symbol string `json:"symbol"`          // ex.: "BTC_USDT"
		Price  string `json:"last_price"`      // ex.: "23117.40"
		Volume string `json:"base_volume_24h"` // ex.: "20674.35254"
		Time   int64  `json:"timestamp"`       // ex.: 1675862097605
	}
)

func NewBitmartProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitmartProvider, error) {
	provider := &BitmartProvider{}
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

func (p *BitmartProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.Join("_")] = pair.String()
	}

	url := p.endpoints.Rest + "/spot/v2/ticker"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickersResponse BitmartTickersResponse
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickersResponse.Data.Tickers {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   time.UnixMilli(ticker.Time),
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
