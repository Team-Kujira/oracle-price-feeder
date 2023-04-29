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
		Http: 	      []string{"https://api.kujira.app", "https://api-kujira.mintthemoon.xyz"},
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
		Price       string `json:"last_price"`      // ex.: "2.0690000418"
		BaseVolume  string `json:"base_volume"`     // ex.: "4875.4890980000"
		QuoteVolume string `json:"target_volume"`   // ex.: "4875.4890980000"
		Base        string `json:"base_currency"`   // ex.: "LUNA"
		Quote       string `json:"target_currency"` // ex.: "axlUSDC"
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
	content, err := p.httpGet("/api/coingecko/tickers")
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
		reciprocal := false
		volume := ticker.BaseVolume

		_, ok := p.pairs[symbol]
		if !ok {
			symbol = quote + base
			reciprocal = true
			volume = ticker.QuoteVolume
			_, ok = p.pairs[symbol]
			if !ok {
				continue
			}
		}

		price := strToDec(ticker.Price)
		if reciprocal {
			price = strToDec("1").Quo(price)
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  price,
			Volume: strToDec(volume),
			Time:   timestamp,
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func finTranslateProviderSymbol(symbol string) string {
	return strings.ToUpper(strings.Replace(symbol, "axl", "", 1))
}
