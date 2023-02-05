package provider

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                   Provider = (*FinProvider)(nil)
	finDefaultEndpoints          = Endpoint{
		Name:         ProviderOsmosisV2,
		Rest:         "https://api.kujira.app/",
		PollInterval: 3 * time.Second,
	}
)

type (
	// FinProvider defines an oracle provider implemented by the FIN
	// public API.
	//
	// REF: ???
	FinProvider struct {
		provider
	}

	FinTickersResponse struct {
		Tickers []FinTicker `json:"pools"`
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
	symbols := make(map[string]bool, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.String()] = true
	}

	url := p.endpoints.Rest + "/api/coingecko/tickers"

	response, err := p.http.Get(url)
	if err != nil {
		p.logger.Warn().
			Err(err).
			Msg("fin failed requesting tickers")
		return err
	}

	if response.StatusCode != 200 {
		p.logger.Warn().
			Int("code", response.StatusCode).
			Msg("fin tickers request returned invalid status")
	}

	content, err := ioutil.ReadAll(response.Body)
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
		base := strings.Replace(ticker.Base, "axl", "", 1)
		quote := strings.Replace(ticker.Quote, "axl", "", 1)
		symbol := base + quote

		_, ok := symbols[symbol]
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
