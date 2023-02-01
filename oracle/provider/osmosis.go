package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                       Provider = (*OsmosisProvider)(nil)
	osmosisDefaultEndpoints          = Endpoint{
		Name:         ProviderGate,
		Rest:         "https://api.osmosis.zone",
		PollInterval: 6 * time.Second,
	}
)

type (
	// OsmosisProvider defines an oracle provider implemented by the Gate.io
	// public API.
	//
	// REF: https://www.gate.io/docs/developers/apiv4/en/
	OsmosisProvider struct {
		provider
	}

	OsmosisPairsResponse struct {
		Time int64         `json:"updated_at"`
		Data []OsmosisPair `json:"data"`
	}

	OsmosisPair struct {
		Base   string  `json:"base_symbol"`     // Base symbol ex.: "ATOM"
		Quote  string  `json:"quote_symbol"`    // Quote symbol ex.: "OSMO"
		Price  float64 `json:"price"`           // Last price ex.: 0.07204
		Volume float64 `json:"base_volume_24h"` // Total traded base asset volume ex.: 167107.988
	}
)

func NewOsmosisProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OsmosisProvider, error) {
	provider := &OsmosisProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)
	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *OsmosisProvider) Poll() error {
	symbols := make(map[string]bool, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.String()] = true
	}

	url := p.endpoints.Rest + "/pairs/v1/summary"

	response, err := p.http.Get(url)
	if err != nil {
		p.logger.Warn().
			Err(err).
			Msg("osmosis failed requesting tickers")
		return err
	}

	if response.StatusCode != 200 {
		p.logger.Warn().
			Int("code", response.StatusCode).
			Msg("osmosis tickers request returned invalid status")
	}

	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	var pairsResponse OsmosisPairsResponse
	err = json.Unmarshal(content, &pairsResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Unix(pairsResponse.Time, 0)

	for _, pair := range pairsResponse.Data {
		symbol := pair.Base + pair.Quote

		_, ok := symbols[symbol]
		if !ok {
			continue
		}

		fmt.Println(pair)

		p.tickers[symbol] = types.TickerPrice{
			Price:  sdk.NewDec(1).Quo(floatToDec(pair.Price)),
			Volume: floatToDec(pair.Volume),
			Time:   timestamp,
		}

		fmt.Println(p.tickers[symbol])
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
