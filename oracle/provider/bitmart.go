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
		Urls:         []string{"https://api-cloud.bitmart.com"},
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

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBitmartSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BitmartProvider) getTickers() ([][]string, error) {
	content, err := p.httpGet("/spot/quotation/v3/tickers")
	if err != nil {
		return nil, err
	}

	var response struct {
		Data [][]string `json:"data"`
	}
	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

func (p *BitmartProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		symbol := ticker[0]
		price := strToDec(ticker[1])

		if !p.isPair(symbol) {
			continue
		}

		volume := strToDec(ticker[2])
		_, found := p.inverse[symbol]
		if found {
			volume = strToDec(ticker[3])
			if !volume.IsZero() {
				volume = volume.Quo(price)
			}
		}

		p.setTickerPrice(
			symbol,
			price,
			volume,
			now,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BitmartProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers {
		symbols[ticker[0]] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToBitmartSymbol(pair types.CurrencyPair) string {
	return pair.Join("_")
}
