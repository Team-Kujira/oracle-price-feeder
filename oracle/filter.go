package oracle

import (
	"price-feeder/oracle/provider"

	"price-feeder/oracle/types"

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
				logger.Debug().
					Str("base", base).
					Str("provider", providerName.String()).
					Str("price", tp.Price.String()).
					Str("mean", means[base].String()).
					Str("margin", d.Mul(t).String()).
					Msg("deviating price")
			}
		}
	}

	return filteredPrices, nil
}

func isBetween(p, mean, margin sdk.Dec) bool {
	return p.GTE(mean.Sub(margin)) &&
		p.LTE(mean.Add(margin))
}

func FilterTickerDeviations2(
	logger zerolog.Logger,
	symbol string,
	tickerPrices map[provider.Name]types.TickerPrice,
	deviationThreshold sdk.Dec,
) (map[provider.Name]types.TickerPrice, error) {
	if deviationThreshold.IsNil() {
		deviationThreshold = defaultDeviationThreshold
	}

	prices := []sdk.Dec{}
	for _, tickerPrice := range tickerPrices {
		prices = append(prices, tickerPrice.Price)
	}

	deviation, mean, err := StandardDeviation2(prices)
	if err != nil {
		return tickerPrices, err
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
