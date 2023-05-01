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
	_                      Provider = (*FinUskProvider)(nil)
	finUskDefaultEndpoints          = Endpoint{
		Name: ProviderFinUsk,
		Urls: []string{
			"https://cosmos.directory/kujira",
			"https://lcd.kaiyo.kujira.setten.io",
			"https://lcd-kujira.mintthemoon.xyz",
		},
		PollInterval: 3 * time.Second,
	}
)

type (
	// FinUsk defines an oracle provider that uses the API of an Kujira node
	// directly to retrieve the "USK/USDC" price
	// It is meant as a temporary solution and does not support any other pairs
	FinUskProvider struct {
		provider
	}

	FinUskBookResponse struct {
		Data FinUskBookData `json:"data"`
	}

	FinUskBookData struct {
		Base  []FinUskOrder `json:"base"`
		Quote []FinUskOrder `json:"quote"`
	}

	FinUskOrder struct {
		Price string `json:"quote_price"`
	}
)

func NewFinUskProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*FinUskProvider, error) {
	provider := &FinUskProvider{}
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

func (p *FinUskProvider) Poll() error {
	_, found := p.pairs["USKUSDC"]
	if !found {
		return nil
	}

	content, err := p.httpGet("/cosmwasm/wasm/v1/contract/kujira1rwx6w02alc4kaz7xpyg3rlxpjl4g63x5jq292mkxgg65zqpn5llq202vh5/smart/eyJib29rIjp7ImxpbWl0IjoxfX0K")
	if err != nil {
		return err
	}

	var bookResponse FinUskBookResponse
	err = json.Unmarshal(content, &bookResponse)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	if len(bookResponse.Data.Base) < 1 || len(bookResponse.Data.Quote) < 1 {
		return fmt.Errorf("no order found")
	}

	base := strToDec(bookResponse.Data.Base[0].Price)
	quote := strToDec(bookResponse.Data.Quote[0].Price)

	price := base.Add(quote).QuoInt64(2)

	// contract has axlUSDC as base and USK as quote, so switch that
	price = strToDec("1").Quo(price)

	p.tickers["USKUSDC"] = types.TickerPrice{
		Price:  price,
		Volume: strToDec("1"),
		Time:   timestamp,
	}

	p.logger.Debug().Msg("updated USK")
	return nil
}
