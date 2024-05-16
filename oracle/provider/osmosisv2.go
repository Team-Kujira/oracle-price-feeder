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
		Urls:         []string{},
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

	provider.decimals = map[string]int64{
		"KUJI":   6,
		"USDC":   6,
		"USK":    6,
		"MNTA":   6,
		"ATOM":   6,
		"OSMO":   6,
		"SOMM":   6,
		"JUNO":   6,
		"WHALE":  6,
		"STATOM": 6,
	}

	symbols := []string{}
	for _, pair := range pairs {
		skip := false
		for _, symbol := range []string{pair.Base, pair.Quote} {
			_, found := provider.decimals[symbol]
			if !found {
				skip = true
				logger.Debug().
					Str("symbol", symbol).
					Msg("unknown decimal")
			}
		}
		if skip {
			continue
		}

		symbols = append(symbols, pair.Base+pair.Quote)
		symbols = append(symbols, pair.Quote+pair.Base)
	}

	volumes, err := volume.NewVolumeHandler(logger, db, "osmosisv2", symbols, 60*60*24)
	if err != nil {
		return provider, err
	}

	provider.volumes = volumes

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

			price, err = p.queryConcentratedLiquidityPool(poolId)
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
			volume, _ = p.volumes.Get(pair.Quote + pair.Base)
			if !volume.IsZero() {
				volume = volume.Quo(price)
			}
		} else {
			volume, _ = p.volumes.Get(pair.String())
		}

		if volume.IsNil() {
			volume = sdk.ZeroDec()
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
			p.denoms[response.Pool.Assets[0].Token.Denom] = pair.Base
			p.denoms[response.Pool.Assets[1].Token.Denom] = pair.Quote
		case "/osmosis.gamm.poolmodels.stableswap.v1beta1.Pool":
			p.denoms[pair.Base] = response.Pool.Liquidity[0].Denom
			p.denoms[pair.Quote] = response.Pool.Liquidity[1].Denom
			p.denoms[response.Pool.Liquidity[0].Denom] = pair.Base
			p.denoms[response.Pool.Liquidity[1].Denom] = pair.Quote
		case "/osmosis.concentratedliquidity.v1beta1.Pool":
			p.denoms[pair.Base] = response.Pool.Token0
			p.denoms[pair.Quote] = response.Pool.Token1
			p.denoms[response.Pool.Token0] = pair.Base
			p.denoms[response.Pool.Token1] = pair.Quote
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

	filter := []string{
		"/osmosis.poolmanager.v1beta1.MsgSwapExactAmountIn",
		"/osmosis.gamm.v1beta1.MsgSwapExactAmountIn",
		"/cosmwasm.wasm.v1.MsgExecuteContract",
		"/ibc.core.channel.v1.MsgRecvPacket", // via skip
		"/osmosis.poolmanager.v1beta1.MsgSplitRouteSwapExactAmountIn",
	}

	txs, timestamp, err := p.getCosmosTxs(height, filter)
	if err != nil {
		return volume.Volume{}, p.error(err)
	}

	// prepare all volumes:
	// not traded pairs have zero volume for this block
	values := map[string]sdk.Dec{}
	for _, symbol := range p.volumes.Symbols() {
		values[symbol] = sdk.ZeroDec()
	}

	for _, tx := range txs {
		swaps := tx.GetEventsByType("token_swapped")
		if len(swaps) == 0 {
			continue
		}

		p.logger.Debug().
			Str("tx", tx.Hash).
			Int("swaps", len(swaps)).
			Msg("swaps found")

		for _, event := range swaps {
			pool, found := event.Attributes["pool_id"]
			if !found {
				continue
			}

			symbol, found := p.contracts[pool]
			if !found {
				p.logger.Debug().
					Str("pool_id", pool).
					Msg("unknown pool")
				continue
			}

			_, found = values[symbol]
			if !found {
				p.logger.Debug().
					Str("symbol", symbol).
					Msg("unknown symbol")
				continue
			}

			in, err := p.getToken(event, "tokens_in")
			if err != nil {
				p.logger.Error().
					Str("symbol", symbol).
					Str("attribute", "tokens_in").
					Msg("failed parsing token")
				continue
			}

			out, err := p.getToken(event, "tokens_out")
			if err != nil {
				p.logger.Error().
					Str("symbol", symbol).
					Str("attribute", "tokens_out").
					Msg("failed parsing token")
				continue
			}

			// needed to for final volumes: {ATOMOSMO: 1, OSMOATOM: 9}
			amounts := map[string]sdk.Dec{
				in.Symbol + out.Symbol: in.Amount,
				out.Symbol + in.Symbol: out.Amount,
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

func (p *OsmosisV2Provider) getToken(
	event types.CosmosTxEvent,
	key string,
) (types.Denom, error) {
	token, found := event.Attributes[key]
	if !found {
		return types.Denom{}, fmt.Errorf("token not found")
	}

	amount, denom, err := parseDenom(token)
	if err != nil {
		return types.Denom{}, err
	}

	symbol, found := p.denoms[denom]
	if !found {
		err := fmt.Errorf("symbol not found")
		p.logger.Err(err).
			Str("denom", denom).Msg("")
		return types.Denom{}, err
	}

	decimals, found := p.decimals[symbol]
	if !found {
		err := fmt.Errorf("no decimals found")
		p.logger.Err(err).Str("symbol", symbol).Msg("")
		return types.Denom{}, err
	}

	amount = amount.Quo(uintToDec(10).Power(uint64(decimals)))

	return types.Denom{
		Amount: amount,
		Symbol: symbol,
	}, nil
}
