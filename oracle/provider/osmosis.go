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
	_                       Provider = (*OsmosisProvider)(nil)
	osmosisDefaultEndpoints          = Endpoint{
		Name:         ProviderOsmosis,
		Rest:         "https://api-osmosis.imperator.co",
		PollInterval: 6 * time.Second,
	}
)

type (
	// OsmosisProvider defines an oracle provider implemented by the
	// imperator.co API.
	//
	// REF: -
	OsmosisProvider struct {
		provider
	}

	OsmosisTicker struct {
		Symbol string  `json:"symbol"`     // ex.: "ATOM"
		Price  float64 `json:"price"`      // ex.: 14.8830587017
		Volume float64 `json:"volume_24h"` // ex.: 6428474.562418117
	}
)

func NewOsmosisProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OsmosisProvider, error) {
	provider := &OsmosisProvider{}
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

func (p *OsmosisProvider) Poll() error {
	denoms := map[string]bool{}
	for _, pair := range p.pairs {
		if pair.Quote == "USD" {
			denoms[strings.ToUpper(pair.Base)] = true
		}
	}

	url := p.endpoints.Rest + "/tokens/v2/all"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickers []OsmosisTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers {
		_, ok := denoms[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[ticker.Symbol+"USD"] = types.TickerPrice{
			Price:  floatToDec(ticker.Price),
			Volume: floatToDec(ticker.Volume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
