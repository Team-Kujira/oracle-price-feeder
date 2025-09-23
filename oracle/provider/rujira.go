package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*RujiraProvider)(nil)
	rujiraDefaultEndpoints          = Endpoint{
		Name:         ProviderRujira,
		Urls:         []string{"https://api.rujira.network"},
		PollInterval: 10 * time.Second,
	}
)

type (
	// RujiraProvider defines an oracle provider using the Rujira API
	RujiraProvider struct {
		provider
	}
)

func NewRujiraProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*RujiraProvider, error) {
	provider := &RujiraProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *RujiraProvider) Poll() error {
	content, err := p.httpGet("/api/trade/tickers")
	if err != nil {
		return err
	}

	var tickers []struct {
		BaseVolume string `json:"base_volume"`
		TickerId   string `json:"ticker_id"`
		LastPrice  string `json:"last_price"`
	}

	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, ticker := range tickers {
		if ticker.TickerId != "LQDY_USDC" {
			continue
		}

		p.setTickerPrice(
			"MNTAUSDC",
			strToDec(ticker.LastPrice),
			strToDec(ticker.BaseVolume),
			timestamp,
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *RujiraProvider) GetAvailablePairs() (map[string]struct{}, error) {
	symbols := map[string]struct{}{
		"MNTAUSDC": {},
	}
	return symbols, nil
}
