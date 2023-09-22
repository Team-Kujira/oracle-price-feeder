package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                         Provider = (*OsmosisV2Provider)(nil)
	osmosisv2DefaultEndpoints          = Endpoint{
		Name:         ProviderOsmosisV2,
		Urls:         []string{"https://rest.cosmos.directory/osmosis"},
		PollInterval: 6 * time.Second,
	}
)

type (
	// OsmosisV2ProviderV2 defines an oracle provider using on chain data from
	// osmosis nodes
	//
	// REF: -
	OsmosisV2Provider struct {
		provider
		denoms       map[string]string
		contracts    map[string]string
		concentrated map[string]struct{}
	}

	OsmosisV2SpotPrice struct {
		Price string `json:"spot_price"`
	}

	OsmosisV2PoolResponse struct {
		Pool OsmosisV2Pool `json:"pool"`
	}

	OsmosisV2Pool struct {
		Type      string                `json:"@type"`
		Assets    [2]OsmosisV2PoolAsset `json:"pool_assets"`
		Liquidity [2]OsmosisV2Token     `json:"pool_liquidity"`
		Token0    string                `json:"token0"`
		Token1    string                `json:"token1"`
		SqrtPrice string                `json:"current_sqrt_price"`
	}

	OsmosisV2PoolAsset struct {
		Token OsmosisV2Token `json:"token"`
	}

	OsmosisV2Token struct {
		Denom string `json:"denom"`
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

	provider.contracts = provider.endpoints.ContractAddresses

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	provider.init()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *OsmosisV2Provider) Poll() error {
	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, pair := range p.getAllPairs() {

		poolId, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no pool id found")
			continue
		}

		var price sdk.Dec

		_, found := p.concentrated[poolId]
		if found {
			_, found := p.inverse[symbol]
			if found {
				pair = pair.Swap()
			}

			price, err = p.queryConcentratedLiquidityPool(pair, poolId)
			if err != nil {
				return err
			}
		} else {
			strPrice, err := p.queryLegacyPool(pair, poolId)
			if err != nil {
				return err
			}
			price = strToDec(strPrice)
		}

		p.setTickerPrice(
			symbol,
			price,
			sdk.ZeroDec(),
			timestamp,
		)
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *OsmosisV2Provider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *OsmosisV2Provider) queryLegacyPool(
	pair types.CurrencyPair,
	poolId string,
) (string, error) {
	baseDenom, found := p.denoms[pair.Base]
	if !found {
		return "", fmt.Errorf("denom not found")
	}

	quoteDenom, found := p.denoms[pair.Quote]
	if !found {
		return "", fmt.Errorf("denom not found")
	}

	// api seems to flipped base and quote
	path := strings.Join([]string{
		"/osmosis/gamm/v1beta1/pools/", poolId,
		"/prices?base_asset_denom=",
		strings.Replace(quoteDenom, "/", "%2F", 1),
		"&quote_asset_denom=",
		strings.Replace(baseDenom, "/", "%2F", 1),
	}, "")

	content, err := p.httpGet(path)
	if err != nil {
		return "", err
	}

	var spotPrice OsmosisV2SpotPrice
	err = json.Unmarshal(content, &spotPrice)
	if err != nil {
		return "", err
	}

	return spotPrice.Price, nil
}

func (p *OsmosisV2Provider) queryConcentratedLiquidityPool(
	pair types.CurrencyPair,
	poolId string,
) (sdk.Dec, error) {
	path := "/osmosis/gamm/v1beta1/pools/" + poolId

	content, err := p.httpGet(path)
	if err != nil {
		return sdk.Dec{}, err
	}

	var response OsmosisV2PoolResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		return sdk.Dec{}, err
	}

	price := strToDec(response.Pool.SqrtPrice).Power(2)

	return price, nil
}

// Get denoms for "legacy" pools, set map for concentrated liquidity
func (p *OsmosisV2Provider) init() error {
	p.denoms = map[string]string{}
	p.concentrated = map[string]struct{}{}

	for symbol, pair := range p.getAllPairs() {
		p.logger.Info().
			Str("symbol", symbol).
			Msg("set denoms")

		pool, found := p.contracts[symbol]
		if !found {
			continue
		}

		path := "/osmosis/gamm/v1beta1/pools/" + pool

		content, err := p.httpGet(path)
		if err != nil {
			return err
		}

		var response OsmosisV2PoolResponse
		err = json.Unmarshal(content, &response)
		if err != nil {
			return err
		}

		switch response.Pool.Type {
		case "/osmosis.gamm.v1beta1.Pool":
			p.denoms[pair.Base] = response.Pool.Assets[0].Token.Denom
			p.denoms[pair.Quote] = response.Pool.Assets[1].Token.Denom
		case "/osmosis.gamm.poolmodels.stableswap.v1beta1.Pool":
			p.denoms[pair.Base] = response.Pool.Liquidity[0].Denom
			p.denoms[pair.Quote] = response.Pool.Liquidity[1].Denom
		case "/osmosis.concentratedliquidity.v1beta1.Pool":
			p.concentrated[pool] = struct{}{}
		default:
			continue
		}
	}

	return nil
}
