package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"price-feeder/oracle/provider/volume"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                         Provider = (*OsmosisV2Provider)(nil)
	osmosisv2DefaultEndpoints          = Endpoint{
		Name:         ProviderOsmosisV2,
		Urls:         []string{"https://rest.cosmos.directory/osmosis"},
		PollInterval: 4 * time.Second,
		VolumeBlocks: 4,
		VolumePause:  0,
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
		volumes      volume.VolumeHandler
		height       uint64
		decimals     map[string]int64
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
	db *sql.DB,
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

	for symbol, contract := range provider.endpoints.ContractAddresses {
		provider.contracts[contract] = symbol
	}

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	volumes, err := volume.NewVolumeHandler(logger, db, "osmosisv2", pairs, 60*60*24)
	if err != nil {
		return provider, err
	}

	provider.volumes = volumes

	provider.decimals = map[string]int64{
		"KUJI":  6,
		"USDC":  6,
		"USK":   6,
		"MNTA":  6,
		"ATOM":  6,
		"OSMO":  6,
		"SOMM":  6,
		"JUNO":  6,
		"WHALE": 6,
	}

	provider.init()

	go startPolling(provider, provider.endpoints.PollInterval, logger)

	return provider, nil
}

func (p *OsmosisV2Provider) Poll() error {
	p.updateVolumes()

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
				p.logger.Error().
					Str("pair", pair.String()).
					Str("pool", poolId).
					Err(err).
					Msg("")

				return err
			}
		} else {
			strPrice, err := p.queryLegacyPool(pair, poolId)
			if err != nil {
				return err
			}
			price = strToDec(strPrice)
		}

		var volume sdk.Dec
		// hack to get the proper volume
		_, found = p.inverse[symbol]
		if found {
			volume = p.volumes.Get(pair.Quote + pair.Base)
			if !volume.IsZero() {
				volume = volume.Quo(price)
			}
		} else {
			volume = p.volumes.Get(pair.String())
		}

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

	price := strToDec(response.Pool.SqrtPrice)
	if price.IsNil() {
		return price, fmt.Errorf("could not parse sqrt price")
	}

	return price.Power(2), nil
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

func (p *OsmosisV2Provider) updateVolumes() {
	missing := p.volumes.GetMissing(p.endpoints.VolumeBlocks)
	missing = append(missing, 0)

	volumes := make([]volume.Volume, len(missing))

	for i, height := range missing {
		volume, err := p.getVolume(height)
		time.Sleep(time.Millisecond * time.Duration(p.endpoints.VolumePause))
		if err != nil {
			p.error(err)
			continue
		}
		volumes[i] = volume
	}

	p.volumes.Add(volumes)
	p.volumes.Debug("OSMOUSDC")
}

func (p *OsmosisV2Provider) getVolume(height uint64) (volume.Volume, error) {
	p.logger.Info().Uint64("height", height).Msg("get volume")

	var err error

	type Denom struct {
		Symbol   string
		Decimals int64
		Amount   sdk.Dec
	}

	if height == 0 {
		height, err = p.getCosmosHeight()
		if err != nil {
			return volume.Volume{}, p.error(err)
		}

		if height == p.height || height == 0 {
			return volume.Volume{}, nil
		}

		p.height = height
	}

	// prepare all volumes:
	// not traded pairs have zero volume for this block
	values := map[string]sdk.Dec{}

	for _, pair := range p.getAllPairs() {
		values[pair.Base+pair.Quote] = sdk.ZeroDec()
		values[pair.Quote+pair.Base] = sdk.ZeroDec()
	}

	txs, timestamp, err := p.getCosmosTxs(height)
	if err != nil {
		return volume.Volume{}, p.error(err)
	}

	for _, tx := range txs {
		trades := tx.GetEventsByType("token_swapped")
		for _, event := range trades {
			pool, found := event.Attributes["pool_id"]
			if !found {
				continue
			}

			fmt.Println(event)

			symbol, found := p.contracts[pool]
			if !found {
				fmt.Println("pool not known:", pool)
				continue
			}

			pair, err := p.getPair(symbol)
			if err != nil {
				p.logger.Warn().Err(err).Str("symbol", symbol).Msg("")
				continue
			}

			fmt.Println(symbol, pair.String())

			tokensIn, found := event.Attributes["tokens_in"]
			if !found {
				continue
			}

			tokensOut, found := event.Attributes["tokens_out"]
			if !found {
				continue
			}

			amountIn, denomIn, err := parseDenom(tokensIn)
			if err != nil {
				p.logger.Err(err).Msg("")
				continue
			}

			amountOut, _, err := parseDenom(tokensOut)
			if err != nil {
				p.logger.Err(err).Msg("")
				continue
			}

			baseDenom, found := p.denoms[pair.Base]
			if !found {
				p.logger.Err(err).Msg("")
				continue
			}

			var symbolIn, symbolOut string

			if denomIn == baseDenom {
				symbolIn = pair.Base
				symbolOut = pair.Quote
			} else {
				symbolIn = pair.Quote
				symbolOut = pair.Base
			}

			decimalsIn, found := p.decimals[symbolIn]
			if !found {
				continue
			}

			decimalsOut, found := p.decimals[symbolOut]
			if !found {
				continue
			}

			ten := uintToDec(10)

			amountIn = amountIn.Quo(ten.Power(uint64(decimalsIn)))
			amountOut = amountOut.Quo(ten.Power(uint64(decimalsOut)))

			// needed to for final volumes: {ATOMOSMO: 1, OSMOATOM: 9}
			amounts := map[string]sdk.Dec{
				symbolIn + symbolOut: amountIn,
				symbolOut + symbolIn: amountOut,
			}

			for symbol, amount := range amounts {
				value, found := values[symbol]
				if !found {
					p.logger.Error().
						Str("symbol", symbol).
						Msg("volume not set")
					continue
				}

				values[symbol] = value.Add(amount)
			}
		}
	}

	volume := volume.Volume{
		Height: height,
		Time:   timestamp.Unix(),
		Values: values,
	}

	return volume, nil
}
