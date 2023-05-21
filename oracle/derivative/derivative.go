package derivative

import (
	"fmt"
	"time"

	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

const (
	DerivativeTvwap  = "tvwap"
	DerivativeStride = "stride"
)

type (
	Derivative interface {
		GetPrice(types.CurrencyPair) (types.TickerPrice, error)
		GetPrices(string) (map[string]types.TickerPrice, error)
	}

	derivative struct {
		pairs   []types.CurrencyPair
		history *history.PriceHistory
		logger  zerolog.Logger
		periods map[string]time.Duration
	}
)

func NewDerivative(
	name string,
	logger zerolog.Logger,
	history *history.PriceHistory,
	pairs []types.CurrencyPair,
	periods map[string]time.Duration,
) (Derivative, error) {
	derivativeLogger := logger.With().Str("derivative", name).Logger()
	switch name {
	case DerivativeStride:
		return NewTvwapDerivative(history, derivativeLogger, pairs, periods)
	case DerivativeTvwap:
		return NewTvwapDerivative(history, derivativeLogger, pairs, periods)
	}
	return nil, fmt.Errorf("unsupported provider: %s", name)
}
