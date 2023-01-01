package oracle

import (
	"price-feeder/oracle/provider"
	"time"

	"price-feeder/oracle/types"

	"github.com/cosmos/cosmos-sdk/telemetry"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

// defaultDeviationThreshold defines how many 𝜎 a provider can be away
// from the mean without being considered faulty. This can be overridden
// in the config.
var defaultDeviationThreshold = sdk.MustNewDecFromStr("1.0")

func FilterStaleTickers(
	logger zerolog.Logger,
	prices provider.AggregatedProviderPrices,
	staleDuration float64, // in seconds
	// excludes []string,
) (provider.AggregatedProviderPrices, error) {
	var filteredPrices = make(provider.AggregatedProviderPrices)

	now := time.Now().UnixMilli()
	staleTime := now - int64(staleDuration*1000)

	for providerName, priceTickers := range prices {
		_, ok := filteredPrices[providerName]

		if !ok {
			filteredPrices[providerName] = make(map[string]types.TickerPrice)
		}

		for base, tp := range priceTickers {
			filteredPrices[providerName][base] = tp
			if tp.Time >= staleTime {
				// filteredPrices[providerName][base] = tp
			} else {
				diff := float64(now-tp.Time) / 1000
				logger.Warn().
					Str("provider", providerName.String()).
					Str("asset", base).
					Float64("age", diff).
					Msg("stale price")
			}
		}
	}

	return filteredPrices, nil
}

// FilterTickerDeviations finds the standard deviations of the prices of
// all assets, and filters out any providers that are not within 2𝜎 of the mean.
func FilterTickerDeviations(
	logger zerolog.Logger,
	prices provider.AggregatedProviderPrices,
	deviationThresholds map[string]sdk.Dec,
) (provider.AggregatedProviderPrices, error) {
	var (
		filteredPrices = make(provider.AggregatedProviderPrices)
		priceMap       = make(map[provider.Name]map[string]sdk.Dec)
	)

	for providerName, priceTickers := range prices {
		p, ok := priceMap[providerName]
		if !ok {
			p = map[string]sdk.Dec{}
			priceMap[providerName] = p
		}
		for base, tp := range priceTickers {
			p[base] = tp.Price
		}
	}

	deviations, means, err := StandardDeviation(priceMap)
	if err != nil {
		return nil, err
	}

	// We accept any prices that are within (2 * T)𝜎, or for which we couldn't get 𝜎.
	// T is defined as the deviation threshold, either set by the config
	// or defaulted to 1.
	for providerName, priceTickers := range prices {
		for base, tp := range priceTickers {
			t := defaultDeviationThreshold
			if _, ok := deviationThresholds[base]; ok {
				t = deviationThresholds[base]
			}

			if d, ok := deviations[base]; !ok || isBetween(tp.Price, means[base], d.Mul(t)) {
				p, ok := filteredPrices[providerName]
				if !ok {
					p = map[string]types.TickerPrice{}
					filteredPrices[providerName] = p
				}
				p[base] = tp
			} else {
				telemetry.IncrCounter(1, "failure", "provider", "type", "ticker")
				logger.Warn().
					Str("base", base).
					Str("provider", providerName.String()).
					Str("price", tp.Price.String()).
					Str("mean", means[base].String()).
					Str("margin", d.Mul(t).String()).
					Msg("provider deviating from other prices")
			}
		}
	}

	return filteredPrices, nil
}

func isBetween(p, mean, margin sdk.Dec) bool {
	return p.GTE(mean.Sub(margin)) &&
		p.LTE(mean.Add(margin))
}
