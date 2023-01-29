package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"strconv"
	"io/ioutil"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

const binanceTickersPath   = "/api/v3/ticker/price"

var (
	_ Provider = (*BinanceProvider)(nil)
	binanceDefaultEndpoints = Endpoint{
		Name: ProviderBinance,
		Rest: "https://api1.binance.com",
		PollInterval: 6 * time.Second,
	}
	binanceUSDefaultEndpoints = Endpoint{
		Name: ProviderBinanceUS,
		Rest: "https://api.binance.us",
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
		Symbol    string `json:"symbol"` // Symbol ex.: BTCUSDT
		LastPrice string `json:"lastPrice"` // Last price ex.: 0.0025
		Volume    string `json:"volume"` // Total traded base asset volume ex.: 1000
	}
)

func NewBinanceProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	binanceUS bool,
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
	go startPolling(provider, endpoints.PollInterval, logger)
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
	tickersResponse, err := p.http.Get(url)
	if err != nil {
		p.logger.Warn().Err(err).Msg("binance failed requesting tickers")
		return err
	}
	if tickersResponse.StatusCode != 200 {
		p.logger.Warn().Int("code", tickersResponse.StatusCode).Msg("binance tickers request returned invalid status")
		if tickersResponse.StatusCode == 429 || tickersResponse.StatusCode == 418 {
			backoffSeconds, err := strconv.Atoi(tickersResponse.Header.Get("Retry-After"))
			if err != nil {
				return err
			}
			p.logger.Warn().Int("seconds", backoffSeconds).Msg("binance ratelimit backoff")
			time.Sleep(time.Duration(backoffSeconds) * time.Second)
			return nil
		}
	}
	tickersContent, err := ioutil.ReadAll(tickersResponse.Body)
	if err != nil {
		return err
	}
	var tickers []BinanceTicker
	err = json.Unmarshal(tickersContent, &tickers)
	if err != nil {
		return err
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		p.tickers[ticker.Symbol] = types.TickerPrice {
			Price: strToDec(ticker.LastPrice),
			Volume: strToDec(ticker.Volume),
			Time: now,
		}
	}
	return nil
}
