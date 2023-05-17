package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_ Provider = (*IdxProvider)(nil)

	idxOsmosisDefaultEndpoints = Endpoint{
		Name:         ProviderIdxOsmosis,
		Urls:         []string{"none.yet"},
		PollInterval: 6 * time.Second,
	}
)

type (
	// BinanceProvider defines an Oracle provider implemented by the Binance public
	// API.
	//
	// REF: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
	// REF: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
	IdxProvider struct {
		provider
		exchange string
	}

	IdxTicker struct {
		Base        string `json:"base_asset"`   // Symbol ex.: ATOM
		Quote       string `json:"quote_asset"`  // Symbol ex.: OSMO
		BaseVolume  string `json:"base_volume"`  // Total traded base asset volume ex.: 5.000000
		QuoteVolume string `json:"quote_volume"` // Total traded base asset volume ex.: 0.022769
		Price       string `json:"price"`        // Last price ex.: 0.0045538
		Time        string `json:"time"`         // Last price ex.: 2023-05-17T07:37:00Z
	}
)

func NewIdxProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*IdxProvider, error) {
	provider := &IdxProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	switch endpoints.Name.String() {
	case "idxosmosis":
		provider.exchange = "osmosis"
	default:
		err := fmt.Errorf("endpoint not supported")
		return nil, err
	}

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToIdxSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *IdxProvider) Poll() error {
	path := fmt.Sprintf("/exchanges/%s/tickers", p.exchange)
	content, err := p.httpGet(path)
	if err != nil {
		return err
	}

	var tickers map[string][]IdxTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range tickers["tickers"] {
		symbol := ticker.Base + "/" + ticker.Quote
		if !p.isPair(symbol) {
			continue
		}

		timestamp, err := time.Parse(time.RFC3339, ticker.Time)
		if err != nil {
			p.logger.Error().
				Str("symbol", symbol).
				Str("timestamp", ticker.Time).
				Msg("failed parsing timestamp")
			continue
		}

		p.setTickerPrice(
			symbol,
			strToDec(ticker.Price),
			strToDec(ticker.BaseVolume),
			timestamp,
		)
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *IdxProvider) GetAvailablePairs() (map[string]struct{}, error) {
	path := fmt.Sprintf("/exchanges/%s/pairs", p.exchange)
	content, err := p.httpGet(path)
	if err != nil {
		return nil, err
	}

	var pairs map[string][]string
	err = json.Unmarshal(content, &pairs)
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, pair := range pairs["pairs"] {
		symbols[pair] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToIdxSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"USDC":     "USDC.axl",
		"USDT":     "USDT.axl",
		"ARUSD":    "arUSD",
		"AXLFIL":   "axlFIL",
		"QATOM":    "qATOM",
		"QOSMO":    "qOSMO",
		"QREGEN":   "qREGEN",
		"QSTARS":   "qSTARS",
		"STATOM":   "stATOM",
		"STEVMOS":  "stEVMOS",
		"STJUNO":   "stJUNO",
		"STLUNA":   "stLUNA",
		"STOSMO":   "stOSMO",
		"STSTARS":  "stSTARS",
		"STKATOM":  "stkATOM",
		"STKDSCRT": "stkd-SCRT",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	return base + "/" + quote
}
