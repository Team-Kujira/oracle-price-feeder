package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                    Provider = (*MayaProvider)(nil)
	mayaDefaultEndpoints          = Endpoint{
		Name:         ProviderMaya,
		Urls:         []string{"https://midgard.mayachain.info"},
		PollInterval: 10 * time.Second,
	}
)

type (
	// MayaProvider defines an oracle provider implemented by the Midgard (Maya)
	// public API.
	//
	// REF: https://midgard.ninerealms.com/v2/doc
	MayaProvider struct {
		provider
		contracts map[string]string
	}

	MayaPool struct {
		Symbol string `json:"asset"`
		Price  string `json:"assetPrice"`
		Volume string `json:"volume24h"`
	}
)

func NewMayaProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*MayaProvider, error) {
	provider := &MayaProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	provider.contracts = provider.endpoints.ContractAddresses

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *MayaProvider) getPools() ([]MayaPool, error) {
	content, err := p.httpGet("/v2/pools")
	if err != nil {
		return nil, err
	}

	var pools []MayaPool
	err = json.Unmarshal(content, &pools)
	if err != nil {
		return nil, err
	}

	return pools, nil
}

func (p *MayaProvider) Poll() error {
	pools, err := p.getPools()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	mapping := map[string]string{}
	for symbol, contract := range p.contracts {
		mapping[contract] = symbol
	}

	precision := int64ToDec(10_000_000_000)

	for _, pool := range pools {
		symbol, found := mapping[pool.Symbol]
		if !found {
			continue
		}

		price := strToDec(pool.Price)
		volume := strToDec(pool.Volume).Quo(precision).Quo(price)

		p.setTickerPrice(
			symbol,
			price,
			volume,
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *MayaProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}
