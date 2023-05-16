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
	_                   Provider = (*OkxProvider)(nil)
	okxDefaultEndpoints          = Endpoint{
		Name:         ProviderOkx,
		Urls:         []string{"https://www.okx.com", "https://aws.okx.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// OkxProvider defines an oracle provider implemented by the OKX
	// public API.
	//
	// REF: https://www.okx.com/docs-v5/en
	OkxProvider struct {
		provider
	}

	OkxTickersResponse struct {
		Code    string      `json:"code"`
		Message string      `json:"msg"`
		Data    []OkxTicker `json:"data"`
	}

	OkxTicker struct {
		Symbol string `json:"instId"` // Symbol ex.: BTC-USDT
		Price  string `json:"last"`   // Last price ex.: 0.0025
		Volume string `json:"vol24h"` // Total traded base asset volume ex.: 1000
		Time   string `json:"ts"`     // Timestamp ex.: 1675246930699
	}
)

func NewOkxProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OkxProvider, error) {
	provider := &OkxProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToOkxSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *OkxProvider) getTickers() (OkxTickersResponse, error) {
	content, err := p.httpGet("/api/v5/market/tickers?instType=SPOT")
	if err != nil {
		return OkxTickersResponse{}, err
	}

	var tickers OkxTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return OkxTickersResponse{}, err
	}

	return tickers, nil
}

func (p *OkxProvider) Poll() error {
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

func (p *OkxProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToOkxSymbol(pair types.CurrencyPair) string {
	return pair.Join("-")
}
