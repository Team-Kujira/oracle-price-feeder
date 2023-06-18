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
	_                     Provider = (*CurveProvider)(nil)
	curveDefaultEndpoints          = Endpoint{
		Name:         ProviderCurve,
		Urls:         []string{"https://api.curve.fi"},
		PollInterval: 10 * time.Second,
	}
)

type (
	// CurveProvider defines an oracle provider implemented by the curve.fi
	// public API.
	//
	// REF: https://github.com/curvefi/curve-api
	CurveProvider struct {
		provider
	}

	CurvePoolsResponse struct {
		Success bool           `json:"success"`
		Data    CurvePoolsData `json:"data"`
	}

	CurvePoolsData struct {
		Pools []CurvePoolData `json:"poolData"`
	}

	CurvePoolData struct {
		Address string      `json:"address"`
		Coins   []CurveCoin `json:"coins"`
	}

	CurveCoin struct {
		Address string  `json:"address"`
		Price   float64 `json:"usdPrice"`
		Symbol  string  `json:"symbol"`
	}

	CurveSubgraphResponse struct {
		Success bool                  `json:"success"`
		Data    CurveSubgraphPoolList `json:"data"`
	}

	CurveSubgraphPoolList struct {
		Pools []CurveSubgraphData `json:"poolList"`
	}

	CurveSubgraphData struct {
		Address string  `json:"address"`
		Volume  float64 `json:"rawVolume"`
	}
)

func NewCurveProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CurveProvider, error) {
	provider := &CurveProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *CurveProvider) Poll() error {
	// get subgraph data, which provides 24h volume data
	// https://api.curve.fi/api/getSubgraphData/ethereum

	content, err := p.httpGet("/api/getSubgraphData/ethereum")
	if err != nil {
		return err
	}

	var subgraphResponse CurveSubgraphResponse
	err = json.Unmarshal(content, &subgraphResponse)
	if err != nil {
		return err
	}

	subgraphs := map[string]CurveSubgraphData{}
	for _, subgraph := range subgraphResponse.Data.Pools {
		subgraphs[strings.ToLower(subgraph.Address)] = subgraph
	}

	maxVolumes := map[string]float64{}
	for _, pair := range p.pairs {
		maxVolumes[pair.Base] = 0
	}

	pools := []CurvePoolData{}

	for _, registryID := range []string{"main", "crypto", "factory"} {
		path := "/api/getPools/ethereum/" + registryID
		content, err = p.httpGet(path)
		if err != nil {
			return err
		}

		var poolsResponse CurvePoolsResponse
		err = json.Unmarshal(content, &poolsResponse)
		if err != nil {
			return err
		}

		pools = append(pools, poolsResponse.Data.Pools...)
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, pool := range pools {
		for _, coin := range pool.Coins {
			denom := strings.ToUpper(coin.Symbol)
			volume, ok := maxVolumes[denom]
			if !ok {
				continue
			}

			subgraph, ok := subgraphs[strings.ToLower(pool.Address)]
			if !ok {
				continue
			}

			if subgraph.Volume < volume {
				continue
			}

			p.setTickerPrice(
				denom+"USD",
				floatToDec(coin.Price),
				floatToDec(subgraph.Volume),
				timestamp,
			)
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *CurveProvider) GetAvailablePairs() (map[string]struct{}, error) {
	symbols := map[string]struct{}{}

	for _, registryID := range []string{"main", "crypto", "factory"} {
		path := "/api/getPools/ethereum/" + registryID
		content, err := p.httpGet(path)
		if err != nil {
			return nil, err
		}

		var poolsResponse CurvePoolsResponse
		err = json.Unmarshal(content, &poolsResponse)
		if err != nil {
			return nil, err
		}

		for _, pool := range poolsResponse.Data.Pools {
			for _, coin := range pool.Coins {
				symbol := strings.ToUpper(coin.Symbol) + "USD"
				symbols[symbol] = struct{}{}
			}
		}
	}

	return symbols, nil
}
