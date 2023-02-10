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
	_                   Provider = (*FinProvider)(nil)
	finDefaultEndpoints          = Endpoint{
		Name:         ProviderFin,
		Rest:         "https://api.kujira.app",
		PollInterval: 3 * time.Second,
	}
)

type (
	// FinProvider defines an oracle provider implemented by the FIN
	// public API.
	//
	// REF: https://docs.kujira.app/dapps-and-infrastructure/fin/coingecko-api
	FinProvider struct {
		provider
	}

	FinTickersResponse struct {
		Tickers []FinTicker `json:"tickers"`
	}

	FinTicker struct {
		Price  string `json:"last_price"`      // ex.: "2.0690000418"
		Volume string `json:"base_volume"`     // ex.: "4875.4890980000"
		Base   string `json:"base_currency"`   // ex.: "LUNA"
		Quote  string `json:"target_currency"` // ex.: "axlUSDC"
	}
)

func NewFinProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*FinProvider, error) {
	provider := &FinProvider{}
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

func (p *FinProvider) Poll() error {
	url := p.endpoints.Rest + "/api/coingecko/tickers"
	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickersResponse FinTickersResponse
	err = json.Unmarshal(content, &tickersResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for _, ticker := range tickersResponse.Tickers {
		base := finTranslateProviderSymbol(ticker.Base)
		quote := finTranslateProviderSymbol(ticker.Quote)
		symbol := base + quote

		_, ok := p.pairs[symbol]
		if !ok {
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

func finTranslateProviderSymbol(symbol string) string {
	return strings.ToUpper(strings.Replace(symbol, "axl", "", 1))
}
