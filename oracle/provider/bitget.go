package provider

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*BitgetProvider)(nil)
	bitgetDefaultEndpoints          = Endpoint{
		Name:         ProviderBitget,
		Urls:         []string{"https://api.bitget.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// BitgetProvider defines an oracle provider implemented by the XT.COM
	// public API.
	//
	// REF: https://bitgetlimited.github.io/apidoc/en/spot
	BitgetProvider struct {
		provider
	}

	BitgetTickersResponse struct {
		Code    string         `json:"code"`
		Message string         `json:"msg"`
		Data    []BitgetTicker `json:"data"`
	}

	BitgetTicker struct {
		Symbol string `json:"symbol"`  // ex.: "BTCUSD"
		Price  string `json:"close"`   // ex.: "24014.11"
		Volume string `json:"baseVol"` // ex.: "7421.5009"
		Time   string `json:"ts"`      // ex.: "1660704288118"
	}
)

func NewBitgetProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitgetProvider, error) {
	provider := &BitgetProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToBitgetSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *BitgetProvider) getTickers() (BitgetTickersResponse, error) {
	content, err := p.httpGet("/api/spot/v1/market/tickers")
	if err != nil {
		return BitgetTickersResponse{}, err
	}

	var tickers BitgetTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return BitgetTickersResponse{}, err
	}

	return tickers, nil
}

func (p *BitgetProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, ticker := range tickers.Data {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		timestamp, err := strconv.ParseInt(ticker.Time, 0, 64)
		if err != nil {
			p.logger.
				Err(err).
				Msg("failed parsing timestamp")
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			time.UnixMilli(timestamp),
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *BitgetProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Data {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToBitgetSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"AXL":   "WAXL",
		"MATIC": "POL",
		"FTM":   "S",
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
