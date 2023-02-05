package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                    Provider = (*MexcProvider)(nil)
	mexcDefaultEndpoints          = Endpoint{
		Name:         ProviderMexc,
		Rest:         "https://api.mexc.com",
		PollInterval: 2 * time.Second,
	}
)

type (
	// MexcProvider defines an oracle provider implemented by the Kucoin
	// public API.
	//
	// REF: https://mxcdevelop.github.io/apidocs/spot_v3_en
	MexcProvider struct {
		provider
	}

	MexcTicker struct {
		Symbol string `json:"symbol"`    // Symbol ex.: BTC-USDT
		Price  string `json:"lastPrice"` // Last price ex.: 0.0025
		Volume string `json:"volume"`    // Total traded base asset volume ex.: 1000
	}
)

func NewMexcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*MexcProvider, error) {
	provider := &MexcProvider{}
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

func (p *MexcProvider) Poll() error {
	symbols := make(map[string]bool, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.String()] = true
	}

	url := p.endpoints.Rest + "/api/v3/ticker/24hr"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickers []MexcTicker
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		_, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}
		p.tickers[ticker.Symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   now,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
