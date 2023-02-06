package provider

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*BitgetProvider)(nil)
	bitgetDefaultEndpoints          = Endpoint{
		Name:         ProviderBitget,
		Rest:         "https://api.bitget.com",
		PollInterval: 2 * time.Second,
	}
)

type (
	// BitgetProvider defines an oracle provider implemented by the XT.COM
	// public API.
	//
	// REF: https://bitgetlimited.github.io/apidoc/en/spot
	BitgetProvider struct {
		provider
	}

	BitgetTickersResponse struct {
		Code    string         `json:"code"`
		Message string         `json:"msg"`
		Data    []BitgetTicker `json:"data"`
	}

	BitgetTicker struct {
		Symbol string `json:"symbol"`  // ex.: "BTCUSD"
		Price  string `json:"close"`   // ex.: "24014.11"
		Volume string `json:"baseVol"` // ex.: "7421.5009"
		Time   string `json:"ts"`      // ex.: "1660704288118"
	}
)

func NewBitgetProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitgetProvider, error) {
	provider := &BitgetProvider{}
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

func (p *BitgetProvider) Poll() error {
	symbols := make(map[string]bool, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.String()] = true
	}

	url := p.endpoints.Rest + "/api/spot/v1/market/tickers"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickers BitgetTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, ticker := range tickers.Data {
		_, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}

		timestamp, err := strconv.ParseInt(ticker.Time, 0, 64)
		if err != nil {
			p.logger.
				Err(err).
				Msg("failed parsing timestamp")
			continue
		}

		p.tickers[ticker.Symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   time.UnixMilli(timestamp),
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
