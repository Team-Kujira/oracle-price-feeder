package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/provider/volume"
	"price-feeder/oracle/types"

	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_ Provider = (*WhitewhaleProvider)(nil)

	whitewhaleCmdxDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleCmdx,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleHuahuaDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleHuahua,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleInjDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleInj,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleJunoDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleJuno,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleLuncDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleLunc,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleLunaDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleLuna,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleSeiDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleSei,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	whitewhaleWhaleDefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleWhale,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
)

type (
	// Whitewhale defines an oracle provider using on chain data from
	// chain specific api nodes
	WhitewhaleProvider struct {
		provider
		assets map[string]WhitewhaleAsset
		denoms map[string]string
	}

	WhitewhaleBalanceResponse struct {
		Balances []WhitewhaleAsset `json:"balances"`
	}

	WhitewhaleAsset struct {
		Denom    string `json:"denom"`
		Amount   string `json:"amount"`
		Decimals uint64
	}

	WhitewhalePairResponse struct {
		Data WhitewhalePairData `json:"data"`
	}

	WhitewhalePairData struct {
		AssetInfos    []WhiteWhaleAssetInfo `json:"asset_infos"`
		AssetDecimals []uint64              `json:"asset_decimals"`
		Type          string                `json:"pair_type"`
	}

	WhiteWhaleAssetInfo struct {
		NativeToken *WhiteWhaleNativeToken `json:"native_token,omitempty"`
		Token       *WhiteWhaleToken       `json:"native,omitempty"`
	}

	WhiteWhaleNativeToken struct {
		Denom string `json:"denom"`
	}

	WhiteWhaleToken struct {
		ContractAddress string `json:"contract_addr,omitempty"`
	}
)

func NewWhitewhaleProvider(
	db *sql.DB,
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*WhitewhaleProvider, error) {
	provider := &WhitewhaleProvider{}
	provider.db = db
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

	provider.assets = provider.getAssets()

	provider.denoms = map[string]string{}
	for symbol, asset := range provider.assets {
		provider.denoms[symbol] = asset.Denom
		provider.denoms[asset.Denom] = symbol
	}

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (r *WhitewhalePairResponse) GetAssets() ([]WhitewhaleAsset, error) {
	assets := make([]WhitewhaleAsset, len(r.Data.AssetInfos))

	for i, info := range r.Data.AssetInfos {
		var denom string

		if info.NativeToken != nil {
			denom = info.NativeToken.Denom
		}

		if info.Token != nil {
			denom = info.Token.ContractAddress
		}

		if denom == "" {
			return nil, fmt.Errorf("no denom found")
		}

		assets[i] = WhitewhaleAsset{
			Denom:    denom,
			Decimals: r.Data.AssetDecimals[i],
		}
	}

	return assets, nil
}

func (p *WhitewhaleProvider) Poll() error {
	if len(p.assets) == 0 {
		p.logger.Warn().Msg("no known assets found")
		return nil
	}

	p.updateVolumes()

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, pair := range p.getAllPairs() {
		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no contract address found")
			continue
		}

		base := p.assets[pair.Base]
		quote := p.assets[pair.Quote]

		path := fmt.Sprintf("/cosmos/bank/v1beta1/balances/%s", contract)

		content, err := p.httpGet(path)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		var balanceResponse WhitewhaleBalanceResponse
		err = json.Unmarshal(content, &balanceResponse)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		var baseAmount, quoteAmount math.LegacyDec
		ten := sdk.NewDec(10)
		for _, asset := range balanceResponse.Balances {
			if asset.Denom == base.Denom {
				baseAmount = strToDec(asset.Amount)
				baseAmount = baseAmount.Quo(ten.Power(base.Decimals))
			}

			if asset.Denom == quote.Denom {
				quoteAmount = strToDec(asset.Amount)
				quoteAmount = quoteAmount.Quo(ten.Power(quote.Decimals))
			}
		}

		if baseAmount.IsNil() {
			p.logger.Error().Msg("base amount is nil")
			continue
		}

		if quoteAmount.IsNil() {
			p.logger.Error().Msg("quote amount is nil")
			continue
		}

		price := quoteAmount.Quo(baseAmount)

		var volume sdk.Dec
		// hack to get the proper volume
		_, found := p.inverse[symbol]
		if found {
			volume, _ = p.volumes.Get(pair.Quote + pair.Base)

			if !volume.IsZero() {
				volume = volume.Quo(price)
			}
		} else {
			volume, _ = p.volumes.Get(pair.String())
		}

		p.setTickerPrice(
			symbol,
			price,
			volume,
			timestamp,
		)
	}

	return nil
}

func (p *WhitewhaleProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *WhitewhaleProvider) getAssets() map[string]WhitewhaleAsset {
	assets := map[string]WhitewhaleAsset{}

	for symbol, pair := range p.getAllPairs() {
		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no contract address found")
			continue
		}

		content, err := p.wasmSmartQuery(contract, `{"pair":{}}`)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		var pairResponse WhitewhalePairResponse
		err = json.Unmarshal(content, &pairResponse)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		_, found := p.pairs[pair.String()]
		if !found {
			pair = pair.Swap()
		}

		pairType := pairResponse.Data.Type
		if pairType != "constant_product" {
			p.logger.Error().
				Str("type", pairType).
				Msg("type not supported")
			continue
		}

		whitewhalePairs, err := pairResponse.GetAssets()
		if err != nil {
			p.logger.Error().Err(err).Msg("")
		}

		assets[pair.Base] = whitewhalePairs[0]
		assets[pair.Quote] = whitewhalePairs[1]
	}

	return assets
}

func (p *WhitewhaleProvider) updateVolumes() {
	missing := p.volumes.GetMissing(p.endpoints.VolumeBlocks)
	missing = append(missing, 0)

	volumes := []volume.Volume{}

	for _, height := range missing {
		volume, err := p.getVolume(height)
		time.Sleep(time.Millisecond * time.Duration(p.endpoints.VolumePause))
		if err != nil {
			p.error(err)
			continue
		}
		volumes = append(volumes, volume)
	}

	p.volumes.Add(volumes)
}

func (p *WhitewhaleProvider) getVolume(height uint64) (volume.Volume, error) {
	p.logger.Info().Uint64("height", height).Msg("get volume")

	var err error

	type Denom struct {
		Symbol   string
		Decimals int
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
	for _, symbol := range p.volumes.Symbols() {
		values[symbol] = sdk.ZeroDec()
	}

	filter := []string{
		"/cosmwasm.wasm.v1.MsgExecuteContract",
	}

	txs, timestamp, err := p.getCosmosTxs(height, filter)
	if err != nil {
		return volume.Volume{}, p.error(err)
	}

	for _, tx := range txs {
		swaps := tx.GetEventsByType("wasm")
		for _, event := range swaps {
			contract, found := event.Attributes["_contract_address"]
			if !found {
				continue
			}

			action, found := event.Attributes["action"]
			if !found || action != "swap" {
				continue
			}

			symbol, found := p.contracts[contract]
			if !found {
				p.logger.Debug().
					Str("contract", contract).
					Msg("unknown contract")
				continue
			}

			_, found = values[symbol]
			if !found {
				p.logger.Debug().
					Str("symbol", symbol).
					Msg("unknown symbol")
				continue
			}

			in := Denom{
				Amount: sdk.ZeroDec(),
			}
			out := Denom{}

			attributes := 0
			keys := []string{
				"offer_asset", "offer_amount", "ask_asset", "return_amount",
				"burn_fee_amount", "protocol_fee_amount", "swap_fee_amount",
			}

			for _, key := range keys {
				value, found := event.Attributes[key]
				if !found {
					break
				}

				switch key {
				case "offer_asset", "ask_asset":
					symbol, found := p.denoms[value]
					if !found {
						break
					}
					asset, found := p.assets[symbol]
					if !found {
						break
					}
					if key == "ask_asset" {
						in.Symbol = symbol
						in.Decimals = int(asset.Decimals)
					} else {
						out.Symbol = symbol
						out.Decimals = int(asset.Decimals)
					}
				default:
					amount := strToDec(value)
					if key == "offer_amount" {
						out.Amount = amount
					} else {
						in.Amount = in.Amount.Add(amount)
					}
				}

				attributes += 1
			}

			if attributes != len(keys) {
				// some value is missing
				continue
			}

			ten := uintToDec(10)

			in.Amount = in.Amount.Quo(ten.Power(uint64(in.Decimals)))
			out.Amount = out.Amount.Quo(ten.Power(uint64(out.Decimals)))

			// needed to for final volumes: {KUJIUSK: 1, USKKUJI: 2}
			denoms := map[string]Denom{
				in.Symbol + out.Symbol: in,
				out.Symbol + in.Symbol: out,
			}

			for symbol, denom := range denoms {
				volume, found := values[symbol]
				if !found {
					p.logger.Error().
						Str("symbol", symbol).
						Msg("volume not set")
					continue
				}

				values[symbol] = volume.Add(denom.Amount)
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
