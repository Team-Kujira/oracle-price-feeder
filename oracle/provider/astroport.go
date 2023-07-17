package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                               Provider = (*AstroportProvider)(nil)
	astroportTerra2DefaultEndpoints          = Endpoint{
		Name:         ProviderAstroportTerra2,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	astroportNeutronDefaultEndpoints = Endpoint{
		Name:         ProviderAstroportNeutron,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
	astroportInjectiveDefaultEndpoints = Endpoint{
		Name:         ProviderAstroportInjective,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
)

type (
	// Astroport defines an oracle provider using on chain data from
	// chain specific api nodes
	AstroportProvider struct {
		provider
		contracts map[string]string
		denoms    map[string]string
	}

	AstroportPairResponse struct {
		Data AstroportPairData `json:"data"`
	}

	AstroportPairData struct {
		AssetInfos []AstroportAsset `json:"asset_infos"`
	}

	AstroportAsset struct {
		NativeToken AstroportNativeToken `json:"native_token"`
	}

	AstroportNativeToken struct {
		Denom string `json:"denom"`
	}

	AstroportSimulationQuery struct {
		Simulation AstroportSimulation `json:"simulation"`
	}

	AstroportSimulation struct {
		OfferAsset AstroportOfferAsset `json:"offer_asset"`
		AskAsset   AstroportAsset      `json:"ask_asset_info"`
	}

	AstroportOfferAsset struct {
		Info   AstroportAsset `json:"info"`
		Amount string         `json:"amount"`
	}

	AstroportSimulationResponse struct {
		Data AstroportSimulationData `json:"data"`
	}

	AstroportSimulationData struct {
		Return     string `json:"return_amount"`
		Spread     string `json:"spread_amount"`
		Commission string `json:"commission_amount"`
	}
)

func NewAstroportProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*AstroportProvider, error) {
	provider := &AstroportProvider{}
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

	provider.denoms = provider.getDenoms()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *AstroportProvider) Poll() error {
	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, pair := range p.getAllPairs() {
		amount := "1000000"

		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no contract address found")
			continue
		}

		msg := AstroportSimulationQuery{
			Simulation: AstroportSimulation{
				OfferAsset: AstroportOfferAsset{
					Info: AstroportAsset{
						NativeToken: AstroportNativeToken{
							Denom: p.denoms[pair.Base],
						},
					},
					Amount: amount,
				},
				AskAsset: AstroportAsset{
					NativeToken: AstroportNativeToken{
						Denom: p.denoms[pair.Quote],
					},
				},
			},
		}

		bz, err := json.Marshal(msg)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		query := base64.StdEncoding.EncodeToString(bz)

		path := fmt.Sprintf(
			"/cosmwasm/wasm/v1/contract/%s/smart/%s",
			contract, query,
		)

		content, err := p.httpGet(path)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		var simulationResponse AstroportSimulationResponse
		err = json.Unmarshal(content, &simulationResponse)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		price := strToDec(simulationResponse.Data.Return).Quo(strToDec(amount))

		p.setTickerPrice(
			symbol,
			price,
			sdk.ZeroDec(),
			timestamp,
		)
	}

	return nil
}

func (p *AstroportProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *AstroportProvider) getDenoms() map[string]string {
	denoms := map[string]string{}

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

		content, err := p.httpGet(path)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		var pairResponse AstroportPairResponse
		err = json.Unmarshal(content, &pairResponse)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		denoms[pair.Base] = pairResponse.Data.AssetInfos[0].NativeToken.Denom
		denoms[pair.Quote] = pairResponse.Data.AssetInfos[1].NativeToken.Denom
	}

	return denoms
}
