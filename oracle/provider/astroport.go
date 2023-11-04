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
		denoms    map[string]AstroportAsset
	}

	AstroportPairResponse struct {
		Data AstroportPairData `json:"data"`
	}

	AstroportPairData struct {
		AssetInfos []AstroportAsset `json:"asset_infos"`
	}

	AstroportAsset struct {
		NativeToken *AstroportNativeToken `json:"native_token,omitempty"`
		Token       *AstroportToken       `json:"token,omitempty"`
	}

	AstroportNativeToken struct {
		Denom string `json:"denom,omitempty"`
	}

	AstroportToken struct {
		ContractAddress string `json:"contract_addr,omitempty"`
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

func (a AstroportAsset) Get() string {
	if a.Token.ContractAddress != "" {
		return a.Token.ContractAddress
	}

	if a.NativeToken.Denom != "" {
		return a.NativeToken.Denom
	}

	return ""
}

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

	fmt.Println(availablePairs)
	fmt.Println(provider.pairs)
	fmt.Println(provider.inverse)

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

		offerAsset, found := p.denoms[pair.Base]
		if !found {
			continue
		}

		askAsset, found := p.denoms[pair.Quote]
		if !found {
			continue
		}

		msg := AstroportSimulationQuery{
			Simulation: AstroportSimulation{
				OfferAsset: AstroportOfferAsset{
					Info:   offerAsset,
					Amount: amount,
				},
				AskAsset: askAsset,
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

		_, found = p.pairs[pair.String()]
		if !found {
			price = floatToDec(1).Quo(price)
		}

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
	fmt.Println("AVAILABLE PAIRS")
	fmt.Println(p.getAvailablePairsFromContracts())
	return p.getAvailablePairsFromContracts()
}

func (p *AstroportProvider) getDenoms() map[string]AstroportAsset {
	assets := map[string]AstroportAsset{}

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

		fmt.Println(pairResponse.Data)

		fmt.Println(pair)

		_, found := p.pairs[pair.String()]
		if !found {
			pair = pair.Swap()
		}

		fmt.Println(found)

		assets[pair.Base] = pairResponse.Data.AssetInfos[0]
		assets[pair.Quote] = pairResponse.Data.AssetInfos[1]

		fmt.Println(assets)
	}

	return assets
}
