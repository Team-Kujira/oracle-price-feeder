package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*DexterProvider)(nil)
	dexterDefaultEndpoints          = Endpoint{
		Name:         ProviderDexter,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
	}
)

type (
	// Astroport defines an oracle provider using on chain data from
	// chain specific api nodes
	DexterProvider struct {
		provider
		contracts map[string]string
		denoms    map[string]string
	}
)

func NewDexterProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*DexterProvider, error) {
	provider := &DexterProvider{}
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

func (p *DexterProvider) Poll() error {
	// {"trade": {"amount_in": "1000000", "amount_out": "499900"}}
	type Response struct {
		Data struct {
			TradeParams struct {
				AmountIn  string `json:"amount_in"`
				AmountOut string `json:"amount_out"`
			} `json:"trade_params"`
		} `json:"data"`
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

		offer, found := p.denoms[pair.Base]
		if !found {
			continue
		}

		ask, found := p.denoms[pair.Quote]
		if !found {
			continue
		}

		message := fmt.Sprintf(`{
			"on_swap": {
				"offer_asset": {
					"native_token": {
						"denom": "%s"
					}
				},
				"ask_asset": {
					"native_token": {
						"denom": "%s"
					}
				},
				"swap_type": {
					"give_in": {}
				},
				"amount": "1000000",
				"max_spread": null,
				"belief_price": "100"
			}
		}`, offer, ask)

		content, err := p.wasmQuery(contract, message)
		if err != nil {
			continue
		}

		var response Response

		err = json.Unmarshal(content, &response)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		amountIn := response.Data.TradeParams.AmountIn
		amountOut := response.Data.TradeParams.AmountOut

		if amountIn == "" || amountOut == "" {
			p.logger.Error().Msg("error simulating swap")
			continue
		}

		price := strToDec(amountOut).Quo(strToDec(amountIn))

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

func (p *DexterProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *DexterProvider) getDenoms() map[string]string {
	assets := map[string]string{}

	// {"assets": [{"info": {"native_token": {"denom": "uxprt"}}}, {...}]}
	type Response struct {
		Data struct {
			Assets [2]struct {
				Info struct {
					Token struct {
						Denom string `json:"denom"`
					} `json:"native_token"`
				} `json:"info"`
			} `json:"assets"`
		} `json:"data"`
	}

	for symbol, pair := range p.getAllPairs() {
		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no contract address found")
			continue
		}

		content, err := p.wasmQuery(contract, `{"config":{}}`)
		if err != nil {
			continue
		}

		var response Response

		err = json.Unmarshal(content, &response)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		_, found := p.pairs[pair.String()]
		if !found {
			pair = pair.Swap()
		}

		assets[pair.Base] = response.Data.Assets[0].Info.Token.Denom
		assets[pair.Quote] = response.Data.Assets[1].Info.Token.Denom
	}

	return assets
}
