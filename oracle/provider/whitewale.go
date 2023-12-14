package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                                 Provider = (*WhitewhaleProvider)(nil)
	WhitewhaleMigalooDefaultEndpoints          = Endpoint{
		Name:         ProviderWhitewhaleMigaloo,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	WhitewhaleTerra2DefaultEndpoints = Endpoint{
		Name:         ProviderWhitewhaleTerra2,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
)

type (
	// Whitewhale defines an oracle provider using on chain data from
	// chain specific api nodes
	WhitewhaleProvider struct {
		provider
		contracts map[string]string
		assets    map[string]WhitewhaleAsset
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

func NewWhitewhaleProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*WhitewhaleProvider, error) {
	provider := &WhitewhaleProvider{}
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

	provider.assets = provider.getAssets()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *WhitewhaleProvider) Poll() error {
	if len(p.assets) == 0 {
		p.logger.Warn().Msg("no known assets founds")
		return nil
	}

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

		p.setTickerPrice(
			symbol,
			price,
			sdk.ZeroDec(),
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

		query := base64.StdEncoding.EncodeToString(
			[]byte(`{"pair":{}}`),
		)

		path := fmt.Sprintf(
			"/cosmwasm/wasm/v1/contract/%s/smart/%s",
			contract, query,
		)

		p.logger.Info().Str("path", path).Msg("")

		content, err := p.httpGet(path)
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
