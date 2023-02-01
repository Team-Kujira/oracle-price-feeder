package provider

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                    Provider = (*GateProvider)(nil)
	gateDefaultEndpoints          = Endpoint{
		Name:         ProviderGate,
		Rest:         "https://api.gateio.ws",
		PollInterval: 2 * time.Second,
	}
)

type (
	// GateProvider defines an oracle provider implemented by the Gate.io
	// public API.
	//
	// REF: https://www.gate.io/docs/developers/apiv4/en/
	GateProvider struct {
		provider
	}

	GateTicker struct {
		Symbol string `json:"currency_pair"` // Symbol ex.: BTC_USDT
		Price  string `json:"last"`          // Last price ex.: 0.0025
		Volume string `json:"base_volume"`   // Total traded base asset volume ex.: 1000
	}
)

func NewGateProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*GateProvider, error) {
	provider := &GateProvider{}
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

func (p *GateProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.Join("_")] = pair.String()
	}

	url := p.endpoints.Rest + "/api/v4/spot/tickers"

	tickersResponse, err := p.http.Get(url)
	if err != nil {
		p.logger.Warn().
			Err(err).
			Msg("gate failed requesting tickers")
		return err
	}

	if tickersResponse.StatusCode != 200 {
		p.logger.Warn().
			Int("code", tickersResponse.StatusCode).
			Msg("gate tickers request returned invalid status")
	}

	tickersContent, err := ioutil.ReadAll(tickersResponse.Body)
	if err != nil {
		return err
	}

	var tickers []GateTicker
	err = json.Unmarshal(tickersContent, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}
		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   now,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
