package oracle

import (
	"price-feeder/oracle/provider"

	"price-feeder/oracle/types"

	"github.com/armon/go-metrics"
	"github.com/cosmos/cosmos-sdk/telemetry"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

// defaultDeviationThreshold defines how many 𝜎 a provider can be away
// from the mean without being considered faulty. This can be overridden
// in the config.
var defaultDeviationThreshold = sdk.MustNewDecFromStr("1.0")

// FilterTickerDeviations finds the standard deviations of the prices of
// all assets, and filters out any providers that are not within 2𝜎 of the mean.

func isBetween(p, mean, margin sdk.Dec) bool {
	return p.GTE(mean.Sub(margin)) &&
		p.LTE(mean.Add(margin))
}

func FilterTickerDeviations(
	logger zerolog.Logger,
	symbol string,
	tickerPrices map[provider.Name]types.TickerPrice,
	deviationThreshold sdk.Dec,
	stats bool,
) (map[provider.Name]types.TickerPrice, error) {
	if deviationThreshold.IsNil() {
		deviationThreshold = defaultDeviationThreshold
	}

	prices := []sdk.Dec{}
	for _, tickerPrice := range tickerPrices {
		prices = append(prices, tickerPrice.Price)
	}

	deviation, mean, err := StandardDeviation(prices)
	if err != nil {
		return tickerPrices, err
	}

	if stats {
		labels := []metrics.Label{
			telemetry.NewLabel("symbol", symbol),
		}

		margin := deviation.Mul(deviationThreshold)

		telemetry.SetGaugeWithLabels(
			[]string{"deviation", "high"},
			float32(mean.Add(margin).MustFloat64()),
			labels,
		)
		telemetry.SetGaugeWithLabels(
			[]string{"deviation", "low"},
			float32(mean.Sub(margin).MustFloat64()),
			labels,
		)
	}

	// We accept any prices that are within (2 * T)𝜎, or for which we couldn't get 𝜎.
	// T is defined as the deviation threshold, either set by the config
	// or defaulted to 1.
	filteredPrices := map[provider.Name]types.TickerPrice{}
	for providerName, tickerPrice := range tickerPrices {
		if isBetween(tickerPrice.Price, mean, deviation.Mul(deviationThreshold)) {
			filteredPrices[providerName] = tickerPrice
		} else {
			telemetry.IncrCounter(1, "failure", "provider", "type", "ticker")
			logger.Debug().
				Str("symbol", symbol).
				Str("provider", providerName.String()).
				Str("price", tickerPrice.Price.String()).
				Str("mean", mean.String()).
				Str("margin", deviation.Mul(deviationThreshold).String()).
				Msg("deviating price")
		}
	}

	return filteredPrices, nil
}
