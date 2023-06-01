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
		Urls:         []string{"https://api.lbkex.com", "https://api.lbank.info", "https://www.lbkex.net"},
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

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToLbankSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *LbankProvider) getTickers() (LbankTickersResponse, error) {
	content, err := p.httpGet("/v2/ticker.do?symbol=all")
	if err != nil {
		return LbankTickersResponse{}, err
	}

	var tickers LbankTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return LbankTickersResponse{}, err
	}

	return tickers, nil
}

func (p *LbankProvider) Poll() error {
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

		p.setTickerPrice(
			ticker.Symbol,
			floatToDec(ticker.Ticker.Price),
			floatToDec(ticker.Ticker.Volume),
			time.UnixMilli(ticker.Time),
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *LbankProvider) GetAvailablePairs() (map[string]struct{}, error) {
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

func currencyPairToLbankSymbol(pair types.CurrencyPair) string {
	return strings.ToLower(pair.Join("_"))
}
