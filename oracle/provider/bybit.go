package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                     Provider = (*BybitProvider)(nil)
	bybitDefaultEndpoints          = Endpoint{
		Name:         ProviderBybit,
		Http:         []string{"https://api.bybit.com", "https://api.bytick/com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// BybitProvider defines an oracle provider implemented by the ByBit
	// public API.
	//
	// REF: https://docs.kujira.app/dapps-and-infrastructure/fin/coingecko-api
	BybitProvider struct {
		provider
	}

	BybitTickersResponse struct {
		Result BybitTickersResult `json:"result"`
	}

	BybitTickersResult struct {
		List []BybitTicker `json:"list"`
	}

	BybitTicker struct {
		Symbol string `json:"symbol"`    // ex.: "LUNAUSDT"
		Price  string `json:"lastPrice"` // ex.: "21127.86"
		Volume string `json:"volume24h"` // ex.: "211.378621"
	}
)

func NewBybitProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BybitProvider, error) {
	provider := &BybitProvider{}
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

func (p *BybitProvider) Poll() error {
	content, err := p.httpGet("/v5/market/tickers?category=spot")
	if err != nil {
		return err
	}

	var tickersResponse BybitTickersResponse
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return err
	}

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickersResponse.Result.List {
		_, ok := p.pairs[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[ticker.Symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
