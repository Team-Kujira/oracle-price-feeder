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
	_                     Provider = (*LbankProvider)(nil)
	lbankDefaultEndpoints          = Endpoint{
		Name:         ProviderLbank,
		Rest:         "https://api.lbkex.com",
		PollInterval: 3 * time.Second,
	}
)

type (
	// LbankProvider defines an oracle provider implemented by the LBank
	// public API.
	//
	// REF: https://www.lbank.com/en-US/docs/index.html
	LbankProvider struct {
		provider
	}

	LbankTickersResponse struct {
		Data []LbankTicker `json:"data"`
	}

	LbankTicker struct {
		Symbol string          `json:"symbol"`    // ex.: "btc_usdt"
		Time   int64           `json:"timestamp"` // ex.: 1675842153413
		Ticker LbankTickerData `json:"ticker"`
	}

	LbankTickerData struct {
		Volume float64 `json:"vol"`    // ex.: 2856.6296
		Price  float64 `json:"latest"` // ex.: 23211.01
	}
)

func NewLbankProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*LbankProvider, error) {
	provider := &LbankProvider{}
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

func (p *LbankProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[strings.ToLower(pair.Join("_"))] = pair.String()
	}

	url := p.endpoints.Rest + "/v2/ticker.do?symbol=all"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickers LbankTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers.Data {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  floatToDec(ticker.Ticker.Price),
			Volume: floatToDec(ticker.Ticker.Volume),
			Time:   time.UnixMilli(ticker.Time),
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
