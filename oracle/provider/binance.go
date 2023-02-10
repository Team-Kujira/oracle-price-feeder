package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

const binanceTickersPath = "/api/v3/ticker"

var (
	_                       Provider = (*BinanceProvider)(nil)
	binanceDefaultEndpoints          = Endpoint{
		Name:         ProviderBinance,
		Rest:         "https://api1.binance.com",
		PollInterval: 6 * time.Second,
	}
	binanceUSDefaultEndpoints = Endpoint{
		Name:         ProviderBinanceUS,
		Rest:         "https://api.binance.us",
		PollInterval: 6 * time.Second,
	}
)

type (
	// BinanceProvider defines an Oracle provider implemented by the Binance public
	// API.
	//
	// REF: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
	// REF: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
	BinanceProvider struct {
		provider
	}

	BinanceTicker struct {
		Symbol    string `json:"symbol"`    // Symbol ex.: BTCUSDT
		LastPrice string `json:"lastPrice"` // Last price ex.: 0.0025
		Volume    string `json:"volume"`    // Total traded base asset volume ex.: 1000
	}
)

func NewBinanceProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BinanceProvider, error) {
	provider := &BinanceProvider{}
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

func (p *BinanceProvider) Poll() error {
	symbols := make([]string, len(p.pairs))
	i := 0
	for symbol := range p.pairs {
		symbols[i] = symbol
		i++
	}
	url := fmt.Sprintf(
		"%s%s?type=MINI&symbols=[\"%s\"]",
		p.endpoints.Rest,
		binanceTickersPath,
		strings.Join(symbols, "\",\""),
	)

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickers []BinanceTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		p.tickers[ticker.Symbol] = types.TickerPrice{
			Price:  strToDec(ticker.LastPrice),
			Volume: strToDec(ticker.Volume),
			Time:   now,
		}
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}
