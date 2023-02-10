package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                        Provider = (*BitfinexProvider)(nil)
	bitfinexDefaultEndpoints          = Endpoint{
		Name:         ProviderBitfinex,
		Rest:         "https://api-pub.bitfinex.com",
		PollInterval: 2500 * time.Millisecond,
	}
)

type (
	// BitfinexProvider defines an oracle provider implemented by the Bitfinex
	// public API.
	//
	// REF: https://docs.bitfinex.com/docs
	BitfinexProvider struct {
		provider
	}
)

func NewBitfinexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitfinexProvider, error) {
	provider := &BitfinexProvider{}
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

func (p *BitfinexProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols["t"+pair.String()] = pair.String()
	}

	url := p.endpoints.Rest + "/v2/tickers?symbols=ALL"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickersResponse [][11]json.RawMessage
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, ticker := range tickersResponse {
		var (
			tickerSymbol string
			tickerPrice  float64
			tickerVolume float64
		)

		err := json.Unmarshal(ticker[0], &tickerSymbol)
		if err != nil {
			p.logger.Error().Msg("failed to parse ticker")
			continue
		}

		symbol, ok := symbols[tickerSymbol]
		if !ok {
			continue
		}

		err = json.Unmarshal(ticker[7], &tickerPrice)
		if err != nil {
			p.logger.Error().Msg("failed to parse price")
			continue
		}

		err = json.Unmarshal(ticker[8], &tickerVolume)
		if err != nil {
			p.logger.Error().Msg("failed to parse volume")
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  floatToDec(tickerPrice),
			Volume: floatToDec(tickerVolume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
