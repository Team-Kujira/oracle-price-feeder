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
		symbols map[string]string
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

	content, err := provider.httpGet("/v2/conf/pub:list:pair:exchange")
	if err != nil {
		return nil, err
	}

	var bitfinexPairs [1][]string
	err = json.Unmarshal(content, &bitfinexPairs)
	if err != nil {
		return nil, err
	}

	provider.symbols = map[string]string{}
	for _, pair := range bitfinexPairs[0] {
		symbol := pair
		symbol = strings.Replace(symbol, "LUNA:", "LUNC:", 1)
		symbol = strings.Replace(symbol, "LUNA2:", "LUNA:", 1)
		symbol = strings.Replace(symbol, ":", "", 1)
		provider.symbols[symbol] = pair
	}

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BitfinexProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		bitfinexSymbol := p.symbols[pair.String()]
		symbols["t"+bitfinexSymbol] = pair.String()
	}

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

		symbol, ok := symbols[tickerSymbol]
		if !ok {
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

		p.tickers[symbol] = types.TickerPrice{
			Price:  floatToDec(tickerPrice),
			Volume: floatToDec(tickerVolume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
