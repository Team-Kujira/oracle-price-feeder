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
	_                  Provider = (*XtProvider)(nil)
	xtDefaultEndpoints          = Endpoint{
		Name:         ProviderXt,
		Urls:         []string{"https://sapi.xt.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// XtProvider defines an oracle provider implemented by the XT.COM
	// public API.
	//
	// REF: hhttps://doc.xt.com/#documentationrestApi
	XtProvider struct {
		provider
	}

	XtTickersResponse struct {
		Code    int64      `json:"rc"`
		Message string     `json:"mc"`
		Result  []XtTicker `json:"result"`
	}

	XtTicker struct {
		Symbol string `json:"s"` // Symbol ex.: "btc_usdt"
		Price  string `json:"c"` // Last price ex.: "0.0025"
		Volume string `json:"q"` // Total traded base asset volume ex.: "1000"
		Time   int64  `json:"t"` // Timestamp ex.: 1675246930699
	}
)

func NewXtProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*XtProvider, error) {
	provider := &XtProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToXtSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *XtProvider) getTickers() (XtTickersResponse, error) {
	content, err := p.httpGet("/v4/public/ticker")
	if err != nil {
		return XtTickersResponse{}, err
	}

	var tickers XtTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return XtTickersResponse{}, err
	}

	return tickers, nil
}

func (p *XtProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers.Result {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			time.UnixMilli(ticker.Time),
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *XtProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Result {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToXtSymbol(pair types.CurrencyPair) string {
	return strings.ToLower(pair.Join("_"))
}
