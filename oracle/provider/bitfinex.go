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
	_                        Provider = (*BitfinexProvider)(nil)
	bitfinexDefaultEndpoints          = Endpoint{
		Name:         ProviderBitfinex,
		Urls:         []string{"https://api-pub.bitfinex.com"},
		PollInterval: 2500 * time.Millisecond,
	}
)

type (
	// BitfinexProvider defines an oracle provider implemented by the Bitfinex
	// public API.
	//
	// REF: https://docs.bitfinex.com/docs
	BitfinexProvider struct {
		provider
	}
)

func NewBitfinexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitfinexProvider, error) {
	provider := &BitfinexProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBitfinexSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BitfinexProvider) Poll() error {
	content, err := p.httpGet("/v2/tickers?symbols=ALL")
	if err != nil {
		return err
	}

	var tickersResponse [][11]json.RawMessage
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, ticker := range tickersResponse {
		var (
			tickerSymbol string
			tickerPrice  float64
			tickerVolume float64
		)

		err := json.Unmarshal(ticker[0], &tickerSymbol)
		if err != nil {
			p.logger.Error().Msg("failed to parse ticker")
			continue
		}

		tickerSymbol = strings.Replace(tickerSymbol, ":", "", 1)
		if !p.isPair(tickerSymbol) {
			continue
		}

		err = json.Unmarshal(ticker[7], &tickerPrice)
		if err != nil {
			p.logger.Error().Msg("failed to parse price")
			continue
		}

		err = json.Unmarshal(ticker[8], &tickerVolume)
		if err != nil {
			p.logger.Error().Msg("failed to parse volume")
			continue
		}

		p.setTickerPrice(
			tickerSymbol,
			floatToDec(tickerPrice),
			floatToDec(tickerVolume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BitfinexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	content, err := p.httpGet("/v2/conf/pub:list:pair:exchange")
	if err != nil {
		return nil, err
	}

	var bitfinexPairs [1][]string
	err = json.Unmarshal(content, &bitfinexPairs)
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, symbol := range bitfinexPairs[0] {
		symbol = "t" + strings.Replace(symbol, ":", "", 1)
		symbols[symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToBitfinexSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"LUNC": "LUNA",
		"LUNA": "LUNA2",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	return "t" + base + quote
}
