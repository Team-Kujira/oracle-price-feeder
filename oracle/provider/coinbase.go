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
	_                        Provider = (*CoinbaseProvider)(nil)
	coinbaseDefaultEndpoints          = Endpoint{
		Name: ProviderCoinbase,
		Rest: "https://api.exchange.coinbase.com",
	}
)

type (
	// CoinbaseProvider defines an oracle provider implemented by the XT.COM
	// public API.
	//
	// REF: https://bitgetlimited.github.io/apidoc/en/spot
	CoinbaseProvider struct {
		provider
	}

	CoinbaseTicker struct {
		Price  string `json:"price"`  // ex.: "24014.11"
		Volume string `json:"volume"` // ex.: "7421.5009"
		Time   string `json:"time"`   // ex.: "1660704288118"
	}
)

func NewCoinbaseProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CoinbaseProvider, error) {
	provider := &CoinbaseProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	interval := time.Duration(len(pairs)/10*2+1) * time.Second

	go startPolling(provider, interval, logger)
	return provider, nil
}

func (p *CoinbaseProvider) Poll() error {
	i := 0
	for _, pair := range p.pairs {
		go func(p *CoinbaseProvider, pair types.CurrencyPair) {
			url := fmt.Sprintf(
				"%s/products/%s/ticker", p.endpoints.Rest, pair.Join("-"),
			)

			content, err := p.makeHttpRequest(url)
			if err != nil {
				return
			}

			var ticker CoinbaseTicker
			err = json.Unmarshal(content, &ticker)
			if err != nil {
				return
			}

			timestamp, err := time.Parse(time.RFC3339, ticker.Time)
			if err != nil {
				p.logger.
					Err(err).
					Msg("failed parsing timestamp")
				return
			}

			p.mtx.Lock()
			defer p.mtx.Unlock()

			p.tickers[pair.String()] = types.TickerPrice{
				Price:  strToDec(ticker.Price),
				Volume: strToDec(ticker.Volume),
				Time:   timestamp,
			}

		}(p, pair)
		// Coinbase has a rate limit of 10req/s, sleeping 1.2s before running
		// the next batch of requests
		i = i + 1
		if i == 10 {
			i = 0
			time.Sleep(time.Millisecond * 1200)
		}
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}
