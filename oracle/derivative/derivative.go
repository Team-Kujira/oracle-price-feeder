package derivative

import (
	"fmt"
	"time"

	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

const (
	DerivativeTvwap = "tvwap"
	DerivativeStride = "stride"
)

type (
	Derivative interface {
		GetPrices(...types.CurrencyPair) (map[string]sdk.Dec, error)
	}

	derivative struct {
		pairs map[string]types.CurrencyPair
		history *history.PriceHistory
		logger zerolog.Logger
		periods map[string]time.Duration
	}
)

func NewDerivative(
	name string,
	logger zerolog.Logger,
	history *history.PriceHistory,
	pairs map[string]types.CurrencyPair,
	periods map[string]time.Duration,
) (Derivative, error) {
	derivativeLogger := logger.With().Str("derivative", name).Logger()
	switch name {
	case DerivativeStride:
	case DerivativeTvwap:
		return NewTvwapDerivative(history, derivativeLogger, pairs, periods)
	}
	return nil, fmt.Errorf("unsupported provider: %s", name)
}