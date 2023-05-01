package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*HitBtcProvider)(nil)
	hitbtcDefaultEndpoints          = Endpoint{
		Name:         ProviderHitBtc,
		Urls:         []string{"https://api.hitbtc.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// HitBtcProvider defines an oracle provider implemented by the HitBTC
	// public API.
	//
	// REF: https://api.hitbtc.com
	HitBtcProvider struct {
		provider
	}

	HitBtcTicker struct {
		Price  string `json:"last"`      // ex.: "2.0690000418"
		Volume string `json:"volume"`    // ex.: "4875.4890980000"
		Time   string `json:"timestamp"` // ex.: "2023-02-07T16:17:56.509Z"
	}
)

func NewHitBtcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HitBtcProvider, error) {
	provider := &HitBtcProvider{}
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

func (p *HitBtcProvider) Poll() error {
	content, err := p.httpGet("/api/3/public/ticker")
	if err != nil {
		return err
	}

	var tickers map[string]HitBtcTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, ticker := range tickers {
		_, ok := p.pairs[symbol]
		if !ok {
			continue
		}

		timestamp, err := time.Parse(
			"2006-01-02T15:04:05.000Z", ticker.Time,
		)
		if err != nil {
			p.logger.
				Err(err).
				Msg("failed parsing timestamp")
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
