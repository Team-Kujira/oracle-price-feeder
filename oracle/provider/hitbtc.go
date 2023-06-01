package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*HitBtcProvider)(nil)
	hitbtcDefaultEndpoints          = Endpoint{
		Name:         ProviderHitBtc,
		Urls:         []string{"https://api.hitbtc.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// HitBtcProvider defines an oracle provider implemented by the HitBTC
	// public API.
	//
	// REF: https://api.hitbtc.com
	HitBtcProvider struct {
		provider
	}

	HitBtcTicker struct {
		Price  string `json:"last"`      // ex.: "2.0690000418"
		Volume string `json:"volume"`    // ex.: "4875.4890980000"
		Time   string `json:"timestamp"` // ex.: "2023-02-07T16:17:56.509Z"
	}
)

func NewHitBtcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HitBtcProvider, error) {
	provider := &HitBtcProvider{}

	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToHitBtcSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *HitBtcProvider) getTickers() (map[string]HitBtcTicker, error) {
	content, err := p.httpGet("/api/3/public/ticker")
	if err != nil {
		return nil, err
	}

	var tickers map[string]HitBtcTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return nil, err
	}

	return tickers, nil
}

func (p *HitBtcProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, ticker := range tickers {
		if !p.isPair(symbol) {
			continue
		}

		timestamp, err := time.Parse(
			"2006-01-02T15:04:05.000Z", ticker.Time,
		)
		if err != nil {
			p.logger.
				Err(err).
				Msg("failed parsing timestamp")
			continue
		}

		p.setTickerPrice(
			symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *HitBtcProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for symbol := range tickers {
		symbols[symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToHitBtcSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"BRL": "BRL20",
		"RUB": "RUB20",
		"TRY": "TRY20",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	return base + quote
}
