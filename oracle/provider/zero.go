package provider

import (
	"context"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_ Provider = (*ZeroProvider)(nil)

	zeroDefaultEndpoints = Endpoint{
		Name:         ProviderZero,
		Urls:         []string{""},
		PollInterval: 3 * time.Second,
	}
)

type (
	// ZeroProvider defines an oracle provider that reports 0 for all pairs
	ZeroProvider struct {
		provider
	}
)

func NewZeroProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*ZeroProvider, error) {
	provider := &ZeroProvider{}
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

func (p *ZeroProvider) Poll() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	timestamp := time.Now()

	for symbol := range p.pairs {
		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec("0"),
			Volume: sdk.NewDec(1),
			Time:   timestamp,
		}
	}

	return nil
}
