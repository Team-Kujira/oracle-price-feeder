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
		Http:         []string{"https://sapi.xt.com"},
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
	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *XtProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[strings.ToLower(pair.Join("_"))] = pair.String()
	}

	content, err := p.httpGet("/v4/public/ticker")
	if err != nil {
		return err
	}

	var tickers XtTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, ticker := range tickers.Result {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   time.UnixMilli(ticker.Time),
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
