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
		pools  map[string]string
		denoms map[string]string
	}

	OsmosisV2SpotPrice struct {
		Price string `json:"spot_price"`
	}

	// OsmosisTicker struct {
	// 	Symbol string  `json:"symbol"`     // ex.: "ATOM"
	// 	Price  float64 `json:"price"`      // ex.: 14.8830587017
	// 	Volume float64 `json:"volume_24h"` // ex.: 6428474.562418117
	// }
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

	provider.denoms = map[string]string{}
	provider.denoms["OSMO"] = "uosmo"
	provider.denoms["STOSMO"] = "ibc/D176154B0C63D1F9C6DCFB4F70349EBF2E2B5A87A05902F57A6AE92B863E9AEC"
	provider.denoms["ATOM"] = "ibc/27394FB092D2ECCD56123C74F36E4C1F926001CEADA9CA97EA622B25F41E5EB2"
	provider.denoms["STATOM"] = "ibc/C140AFD542AE77BD7DCC83F13FDD8C5E5BB8C4929785E6EC2F4C636F98F17901"

	provider.pools = map[string]string{}
	provider.pools["STATOMATOM"] = "803"
	provider.pools["STOSMOOSMO"] = "833"

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *OsmosisV2Provider) Poll() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, pair := range p.pairs {
		poolId, found := p.pools[pair.Base+pair.Quote]
		if !found {
			poolId, found = p.pools[pair.Quote+pair.Base]
			if !found {
				continue
			}
		}

		baseDenom, found := p.denoms[pair.Base]
		if !found {
			continue
		}

		quoteDenom, found := p.denoms[pair.Quote]
		if !found {
			continue
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
			return err
		}

		var spotPrice OsmosisV2SpotPrice
		err = json.Unmarshal(content, &spotPrice)
		if err != nil {
			return err
		}

		p.tickers[pair.String()] = types.TickerPrice{
			Price:  strToDec(spotPrice.Price),
			Volume: strToDec("1"),
			Time:   timestamp,
		}
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}
