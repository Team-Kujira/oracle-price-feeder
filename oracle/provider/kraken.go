package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*KrakenProvider)(nil)
	krakenDefaultEndpoints          = Endpoint{
		Name:         ProviderKraken,
		Urls:         []string{"https://api.kraken.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// KrakenProvider defines an oracle provider implemented by the Kraken
	// public API.
	//
	// REF: https://docs.kraken.com/rest
	KrakenProvider struct {
		provider
	}

	KrakenTickerResponse struct {
		Result map[string]KrakenTicker `json:"result"`
	}

	KrakenTicker struct {
		Price  [2]string `json:"c"` // ex.: ["0.52900","94.23583387"]
		Volume [2]string `json:"v"` // ex.: ["6512.53593495","9341.68221855"]
	}

	KrakenPairsResponse struct {
		Result map[string]KrakenPair `json:"result"`
	}

	KrakenPair struct {
		WsName string `json:"wsname"` // ex.: "XBT/USD"
	}
)

func NewKrakenProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*KrakenProvider, error) {
	provider := &KrakenProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToKrakenSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *KrakenProvider) getTickers() (KrakenTickerResponse, error) {
	content, err := p.httpGet("/0/public/Ticker")
	if err != nil {
		return KrakenTickerResponse{}, err
	}

	var tickers KrakenTickerResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return KrakenTickerResponse{}, err
	}

	return tickers, nil
}

func (p *KrakenProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for tickerSymbol, ticker := range tickers.Result {
		if !p.isPair(tickerSymbol) {
			continue
		}

		p.setTickerPrice(
			tickerSymbol,
			strToDec(ticker.Price[0]),
			strToDec(ticker.Volume[1]),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *KrakenProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for symbol := range tickers.Result {
		symbols[symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToKrakenSymbol(pair types.CurrencyPair) string {
	symbols := map[string]string{
		"USDTUSD": "USDTZUSD",
		"ETCETH":  "XETCXETH",
		"ETCXBT":  "XETCXXBT",
		"ETCEUR":  "XETCZEUR",
		"ETCUSD":  "XETCZUSD",
		"ETHXBT":  "XETHXXBT",
		"ETHCAD":  "XETHZCAD",
		"ETHEUR":  "XETHZEUR",
		"ETHGBP":  "XETHZGBP",
		"ETHJPY":  "XETHZJPY",
		"ETHUSD":  "XETHZUSD",
		"LTCXBT":  "XLTCXXBT",
		"LTCEUR":  "XLTCZEUR",
		"LTCJPY":  "XLTCZJPY",
		"LTCUSD":  "XLTCZUSD",
		"MLNETH":  "XMLNXETH",
		"MLNXBT":  "XMLNXXBT",
		"MLNEUR":  "XMLNZEUR",
		"MLNUSD":  "XMLNZUSD",
		"REPXBT":  "XREPXXBT",
		"REPEUR":  "XREPZEUR",
		"REPUSD":  "XREPZUSD",
		"XBTCAD":  "XXBTZCAD",
		"XBTEUR":  "XXBTZEUR",
		"XBTGBP":  "XXBTZGBP",
		"XBTJPY":  "XXBTZJPY",
		"XBTUSD":  "XXBTZUSD",
		"XDGXBT":  "XXDGXXBT",
		"XLMXBT":  "XXLMXXBT",
		"XLMEUR":  "XXLMZEUR",
		"XLMGBP":  "XXLMZGBP",
		"XLMUSD":  "XXLMZUSD",
		"XMRXBT":  "XXMRXXBT",
		"XMREUR":  "XXMRZEUR",
		"XMRUSD":  "XXMRZUSD",
		"XRPXBT":  "XXRPXXBT",
		"XRPCAD":  "XXRPZCAD",
		"XRPEUR":  "XXRPZEUR",
		"XRPJPY":  "XXRPZJPY",
		"XRPUSD":  "XXRPZUSD",
		"ZECXBT":  "XZECXXBT",
		"ZECEUR":  "XZECZEUR",
		"ZECUSD":  "XZECZUSD",
		"EURUSD":  "ZEURZUSD",
		"GBPUSD":  "ZGBPZUSD",
		"USDCAD":  "ZUSDZCAD",
		"USDJPY":  "ZUSDZJPY",
	}
	mapping := map[string]string{
		"AXL":  "WAXL",
		"BTC":  "XBT",
		"LUNC": "LUNA",
		"LUNA": "LUNA2",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	symbol, found := symbols[base+quote]
	if !found {
		symbol = base + quote
	}

	return symbol
}
