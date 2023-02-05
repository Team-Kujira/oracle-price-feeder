package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                         Provider = (*OsmosisV2Provider)(nil)
	osmosisv2DefaultEndpoints          = Endpoint{
		Name:         ProviderOsmosisV2,
		Rest:         "https://api-osmosis.imperator.co",
		PollInterval: 6 * time.Second,
	}
)

type (
	// OsmosisV2Provider defines an oracle provider implemented by the Gate.io
	// public API.
	//
	// REF: https://www.gate.io/docs/developers/apiv4/en/
	OsmosisV2Provider struct {
		provider
	}

	OsmosisV2PoolsResponse struct {
		Pools []OsmosisV2Pool `json:"pools"`
	}

	OsmosisV2Pool struct {
		Volume float64               `json:"volume_24h"` // Total traded base asset volume ex.: 167107.988
		Tokens [2]OsmosisV2PoolToken `json:"pool_tokens"`
	}

	OsmosisV2PoolToken struct {
		Symbol string  `json:"symbol"`
		Amount float64 `json:"amount"`
		Weight float64 `json:"weight_or_scaling"`
	}
)

func NewOsmosisV2Provider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OsmosisV2Provider, error) {
	provider := &OsmosisV2Provider{}
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

func (p *OsmosisV2Provider) Poll() error {
	symbols := make(map[string]bool, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.String()] = true
	}

	url := p.endpoints.Rest + "/stream/pool/v1/all?min_liquidity=10000&limit=160"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var poolsResponse OsmosisV2PoolsResponse
	err = json.Unmarshal(content, &poolsResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()
	processed := map[string]bool{}

	for _, pool := range poolsResponse.Pools {
		base := pool.Tokens[0]
		quote := pool.Tokens[1]
		symbol := base.Symbol + quote.Symbol

		_, ok := symbols[symbol]
		if !ok {
			continue
		}

		// skip, if a pool with the same base/quote pair has been processed
		// because the pools are sorted by liquidity, the previous pool had
		// more liquidity and should be counted
		_, ok = processed[symbol]
		if ok {
			continue
		}

		rate := (quote.Amount / quote.Weight) / (base.Amount / base.Weight)

		p.tickers[symbol] = types.TickerPrice{
			Price:  floatToDec(rate),
			Volume: floatToDec(pool.Volume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
