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
	_                     Provider = (*HuobiProvider)(nil)
	huobiDefaultEndpoints          = Endpoint{
		Name:         ProviderHuobi,
		Http:         []string{"https://api.huobi.pro", "https://api-aws.huobi.pro"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// HuobiProvider defines an oracle provider implemented by the crypto.com
	// public API.
	//
	// REF: https://huobiapi.github.io/docs/spot/v1/en
	HuobiProvider struct {
		provider
	}

	HuobiTickersResponse struct {
		Data []HuobiTicker `json:"data"`
	}

	HuobiTicker struct {
		Symbol string  `json:"symbol"` // Symbol ex.: btcusdt
		Price  float64 `json:"close"`  // Last price ex.: 0.0025
		Volume float64 `json:"amount"` // Total traded base asset volume ex.: 1000
	}
)

func NewHuobiProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HuobiProvider, error) {
	provider := &HuobiProvider{}
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

func (p *HuobiProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[strings.ToLower(pair.String())] = pair.String()
	}

	content, err := p.httpGet("/market/tickers")
	if err != nil {
		return err
	}

	var tickers HuobiTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers.Data {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  floatToDec(ticker.Price),
			Volume: floatToDec(ticker.Volume),
			Time:   now,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
