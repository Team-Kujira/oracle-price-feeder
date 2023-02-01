package derivative

import (
	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

const (
	DerivativeTvwap = "tvwap"
	DerivativeStride = "stride"
)

type (
	Derivative interface {
		GetTickerPrices(...types.CurrencyPair) (map[string]types.TickerPrice, error)
	}

	derivative struct {
		pairs map[string]types.CurrencyPair
		history *history.PriceHistory
		logger zerolog.Logger
	}
)