package derivative

import (
	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

type (
	Derivative interface {
		GetTickerPrices(...types.CurrencyPair) (map[string]types.TickerPrice, error)
	}

	derivative struct {
		denom string
		history *history.PriceHistory
		pairs map[string]types.CurrencyPair
		logger zerolog.Logger
	}
)