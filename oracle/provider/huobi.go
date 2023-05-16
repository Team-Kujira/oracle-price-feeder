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
	_                     Provider = (*HuobiProvider)(nil)
	huobiDefaultEndpoints          = Endpoint{
		Name:         ProviderHuobi,
		Urls:         []string{"https://api.huobi.pro", "https://api-aws.huobi.pro"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// HuobiProvider defines an oracle provider implemented by the crypto.com
	// public API.
	//
	// REF: https://huobiapi.github.io/docs/spot/v1/en
	HuobiProvider struct {
		provider
	}

	HuobiTickersResponse struct {
		Data []HuobiTicker `json:"data"`
	}

	HuobiTicker struct {
		Symbol string  `json:"symbol"` // Symbol ex.: btcusdt
		Price  float64 `json:"close"`  // Last price ex.: 0.0025
		Volume float64 `json:"amount"` // Total traded base asset volume ex.: 1000
	}
)

func NewHuobiProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HuobiProvider, error) {
	provider := &HuobiProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToHuobiSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *HuobiProvider) getTickers() (HuobiTickersResponse, error) {
	content, err := p.httpGet("/market/tickers")
	if err != nil {
		return HuobiTickersResponse{}, err
	}

	var tickers HuobiTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return HuobiTickersResponse{}, err
	}

	return tickers, nil
}

func (p *HuobiProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()

	for _, ticker := range tickers.Data {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			floatToDec(ticker.Price),
			floatToDec(ticker.Volume),
			now,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *HuobiProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, nil
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Data {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToHuobiSymbol(pair types.CurrencyPair) string {
	return strings.ToLower(pair.String())
}
