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
	_ Provider = (*OsmosisProvider)(nil)

	osmosisDefaultEndpoints = Endpoint{
		Name: ProviderOsmosis,
		Urls: []string{
			"https://api.osmosis.zone",
			"https://api-osmosis.imperator.co",
		},
		PollInterval: 6 * time.Second,
	}
)

type (
	// OsmosisProvider defines an oracle provider implemented by the
	// imperator.co API.
	//
	// REF: -
	OsmosisProvider struct {
		provider
	}

	OsmosisTicker struct {
		Symbol string  `json:"symbol"`     // ex.: "ATOM"
		Price  float64 `json:"price"`      // ex.: 14.8830587017
		Volume float64 `json:"volume_24h"` // ex.: 6428474.562418117
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

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *OsmosisProvider) getTickers() ([]OsmosisTicker, error) {
	content, err := p.httpGet("/tokens/v2/all")
	if err != nil {
		return nil, err
	}

	var tickers []OsmosisTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *OsmosisProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers {
		symbol := strings.ToUpper(ticker.Symbol) + "USD"
		if !p.isPair(symbol) {
			continue
		}

		p.setTickerPrice(
			symbol,
			floatToDec(ticker.Price),
			floatToDec(ticker.Volume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *OsmosisProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers {
		symbol := strings.ToUpper(ticker.Symbol) + "USD"
		symbols[symbol] = struct{}{}
	}

	return symbols, nil
}
