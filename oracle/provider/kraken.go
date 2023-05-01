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
		symbols map[string]string
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

	content, err := provider.httpGet("/0/public/AssetPairs")
	if err != nil {
		return nil, err
	}

	var krakenPairs KrakenPairsResponse
	err = json.Unmarshal(content, &krakenPairs)
	if err != nil {
		return nil, err
	}

	provider.symbols = map[string]string{}
	for symbol, pair := range krakenPairs.Result {
		values := strings.Split(pair.WsName, "/")
		base := values[0]
		quote := values[1]
		switch quote {
		case "XBT":
			quote = "BTC"
		case "ZUSD":
			quote = "USD"
		}

		switch base {
		case "XBT":
			base = "BTC"
		case "LUNA":
			base = "LUNC"
		case "LUNA2":
			base = "LUNA"
		}

		provider.symbols[base+quote] = symbol
	}

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *KrakenProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		krakenSymbol := p.symbols[pair.String()]
		symbols[krakenSymbol] = pair.String()
	}

	content, err := p.httpGet("/0/public/Ticker")
	if err != nil {
		return err
	}

	var tickers KrakenTickerResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for tickerSymbol, ticker := range tickers.Result {
		symbol, ok := symbols[tickerSymbol]
		if !ok {
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price[0]),
			Volume: strToDec(ticker.Volume[1]),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
