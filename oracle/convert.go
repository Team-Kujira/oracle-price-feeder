package oracle

import (
	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

// convertTickersToUSD converts any tickers which are not quoted in USD to USD,
// using the conversion rates of other tickers. It will also filter out any tickers
// not within the deviation threshold set by the config.
//
// Ref: https://github.com/umee-network/umee/blob/4348c3e433df8c37dd98a690e96fc275de609bc1/price-feeder/oracle/filter.go#L41
func convertTickersToUSD(
	logger zerolog.Logger,
	providerPrices provider.AggregatedProviderPrices,
	providerPairs map[provider.Name][]types.CurrencyPair,
	deviationThresholds map[string]sdk.Dec,
	providerMinOverrides map[string]int64,
) (map[string]sdk.Dec, error) {

	if len(providerPrices) == 0 {
		return nil, nil
	}

	// group ticker prices by symbol

	providerPricesBySymbol := map[string]map[provider.Name]types.TickerPrice{}
	for providerName, tickerPrices := range providerPrices {
		for symbol, tickerPrice := range tickerPrices {
			_, found := providerPricesBySymbol[symbol]
			if !found {
				providerPricesBySymbol[symbol] = map[provider.Name]types.TickerPrice{}
			}

			providerPricesBySymbol[symbol][providerName] = tickerPrice
		}
	}

	symbols := map[string]struct{}{}
	pairs := []types.CurrencyPair{}
	for _, currencyPairs := range providerPairs {
		for _, currencyPair := range currencyPairs {
			symbol := currencyPair.String()
			_, found := symbols[symbol]
			if !found {
				symbols[symbol] = struct{}{}
				pairs = append(pairs, currencyPair)
			}
		}
	}

	// calculate USD values

	// more than 6 conversions for the USD price is probably not very accurate
	maxConversions := 6
	usdRates := map[string]map[provider.Name]types.TickerPrice{}

	for i := 0; i < maxConversions; i++ {
		unresolved := []types.CurrencyPair{}
		// sort.Slice(vwaps, func(i, j int) bool {
		// 	volume1 := vwaps[i].Volume.Mul(vwaps[i].Value)
		// 	volume2 := vwaps[j].Volume.Mul(vwaps[j].Value)
		// 	return volume1.GT(volume2)
		// })

		for _, currencyPair := range pairs {
			symbol := currencyPair.String()
			base := currencyPair.Base
			quote := currencyPair.Quote

			threshold := deviationThresholds[base]

			tickerPrices := providerPricesBySymbol[symbol]

			if quote == "USD" {
				newRates, err := addRates(
					logger,
					symbol,
					threshold,
					usdRates[base],
					tickerPrices,
				)
				if err != nil {
					if len(tickerPrices) >= 3 {
						return nil, err
					}
				}
				usdRates[base] = newRates
			} else {
				rates, found := usdRates[quote]
				if !found {
					unresolved = append(unresolved, currencyPair)
					continue
				}

				rate, err := vwapRate(rates)
				if err != nil {
					return nil, err
				}

				newRates := map[provider.Name]types.TickerPrice{}
				for providerName, tickerPrice := range tickerPrices {
					newRates[providerName] = types.TickerPrice{
						Price:  tickerPrice.Price.Mul(rate),
						Volume: tickerPrice.Volume,
						Time:   tickerPrice.Time,
					}
				}

				newRates, err = addRates(
					logger,
					symbol,
					threshold,
					usdRates[base],
					newRates,
				)
				if err != nil {
					if len(tickerPrices) >= 3 {
						return nil, err
					}
				}
				usdRates[base] = newRates
			}
		}

		// Stop if there are no unresolved symbols left or no symbol could
		// be converted, in which case the list of unresolved symbols is
		// the same as the list of pairs
		if len(unresolved) == 0 || len(unresolved) == len(pairs) {
			break
		}

		// Try the next round with all unresolved symbols
		pairs = []types.CurrencyPair{}
		pairs = append(pairs, unresolved...)
	}

	ratesDec := map[string]sdk.Dec{}
	for denom, tickers := range usdRates {
		for name, ticker := range tickers {
			provider.TelemetryProviderPrice(
				provider.Name("_"+name.String()),
				denom+"USD",
				float32(ticker.Price.MustFloat64()),
				float32(ticker.Volume.MustFloat64()),
			)
		}

		threshold := deviationThresholds[denom]
		filtered, err := FilterTickerDeviations(
			logger, denom, tickers, threshold,
		)
		if err != nil {
			minimum, found := providerMinOverrides[denom]
			if !found {
				logger.Err(err)
				continue
			}
			if int64(len(filtered)) < minimum {
				logger.Info().
					Str("denom", denom).
					Int64("minimum", minimum).
					Int("available", len(filtered)).
					Msg("not enough tickers")
				continue
			}
		}

		rate, err := vwapRate(filtered)
		if err != nil {
			logger.Err(err)
			continue
		}

		if rate.IsZero() {
			logger.Error().
				Str("denom", denom).
				Msg("rate is zero")
			continue
		}

		ratesDec[denom] = rate

		provider.TelemetryProviderPrice(
			"_final",
			denom+"USD",
			float32(rate.MustFloat64()),
			float32(1),
		)
	}

	return ratesDec, nil
}

func addRates(
	logger zerolog.Logger,
	symbol string,
	threshold sdk.Dec,
	rates map[provider.Name]types.TickerPrice,
	tickers map[provider.Name]types.TickerPrice,
) (map[provider.Name]types.TickerPrice, error) {
	if rates == nil {
		rates = map[provider.Name]types.TickerPrice{}
	}
	for providerName, tickerPrice := range tickers {
		// Don't add new calculated USD price if there is already one from
		// the same provider
		_, found := rates[providerName]
		if found {
			logger.Info().
				Str("provider", providerName.String()).
				Str("symbol", symbol).
				Msg("rate already set for provider")
			continue
		}
		rates[providerName] = tickerPrice
	}
	// Filter outliers
	return FilterTickerDeviations(logger, symbol, rates, threshold)
}

func vwapRate(rates map[provider.Name]types.TickerPrice) (sdk.Dec, error) {
	prices := []types.TickerPrice{}
	for _, price := range rates {
		prices = append(prices, price)
	}
	return ComputeVWAP(prices)
}
