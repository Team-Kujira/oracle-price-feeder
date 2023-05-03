package oracle

import (
	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"
	"sort"

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
	tickers provider.AggregatedProviderPrices,
	providerPairs map[provider.Name][]types.CurrencyPair,
	deviationThresholds map[string]sdk.Dec,
) (map[string]sdk.Dec, error) {

	if len(tickers) == 0 {
		return nil, nil
	}

	type Vwap struct {
		Base   string
		Quote  string
		Value  sdk.Dec
		Volume sdk.Dec
	}

	type Rate struct {
		Value  sdk.Dec
		Volume sdk.Dec
	}

	// prepare map of vwap prices calculated over all providers

	tickerPriceVwaps := map[string]Vwap{}

	for _, pairs := range providerPairs {
		for _, pair := range pairs {
			symbol := pair.String()
			_, found := tickerPriceVwaps[symbol]
			if !found {
				tickerPriceVwaps[symbol] = Vwap{
					Base:  pair.Base,
					Quote: pair.Quote,
				}
			}
		}
	}

	// remove outliers

	providerPrices, err := FilterTickerDeviations(
		logger,
		tickers,
		deviationThresholds,
	)
	if err != nil {
		return nil, err
	}

	// group ticker prices by symbol

	tickerPricesBySymbol := map[string][]types.TickerPrice{}
	for _, tickerPrices := range providerPrices {
		for symbol, tickerPrice := range tickerPrices {
			_, found := tickerPricesBySymbol[symbol]
			if !found {
				tickerPricesBySymbol[symbol] = []types.TickerPrice{}
			}

			tickerPricesBySymbol[symbol] = append(
				tickerPricesBySymbol[symbol],
				tickerPrice,
			)
		}
	}

	// calculate vwap for every symbol

	for symbol, tickerPrices := range tickerPricesBySymbol {
		_, found := tickerPriceVwaps[symbol]

		if !found {
			logger.Error().
				Str("symbol", symbol).
				Msg("Symbol not in providerPairs")
			continue
		}

		vwap, err := ComputeVWAP(tickerPrices)

		if err != nil {
			logger.Error().
				Str("symbol", symbol).
				Msg("Failed computing VWAP")
			continue
		}

		volume := sdk.ZeroDec()
		for _, ticker := range tickerPrices {
			volume = volume.Add(ticker.Volume)
		}

		tickerPriceVwap := tickerPriceVwaps[symbol]

		tickerPriceVwaps[symbol] = Vwap{
			Base:   tickerPriceVwap.Base,
			Quote:  tickerPriceVwap.Quote,
			Value:  vwap,
			Volume: volume,
		}

	}

	vwaps := []Vwap{}
	for _, vwap := range tickerPriceVwaps {
		if vwap.Value.IsNil() || vwap.Value.IsZero() {
			continue
		}
		vwaps = append(vwaps, vwap)
	}

	// calculate USD values

	rates := map[string]Rate{}
	for {
		unresolved := []Vwap{}

		sort.Slice(vwaps, func(i, j int) bool {
			return vwaps[i].Volume.GT(vwaps[j].Volume)
		})

		for _, vwap := range vwaps {
			rate := Rate{Value: sdk.ZeroDec(), Volume: sdk.ZeroDec()}

			if vwap.Quote == "USD" {
				rate.Value = vwap.Value
				rate.Volume = vwap.Volume
			} else {
				quoteRate, found := rates[vwap.Quote]
				if found {
					rate.Value = vwap.Value.Mul(quoteRate.Value)
					rate.Volume = vwap.Volume
				} else {
					unresolved = append(unresolved, vwap)
				}
			}

			if !rate.Value.IsZero() {
				// VWAP
				existing, found := rates[vwap.Base]
				if found {
					total := existing.Value.Mul(existing.Volume)
					total = total.Add(rate.Value.Mul(rate.Volume))
					volume := existing.Volume.Add(rate.Volume)

					rate.Value = total.Quo(volume)
					rate.Volume = volume
				}

				rates[vwap.Base] = rate
			}
		}

		if len(unresolved) == 0 || len(unresolved) == len(vwaps) {
			break
		}

		copy(vwaps, unresolved)
	}

	ratesDec := map[string]sdk.Dec{}
	for denom, rate := range rates {
		ratesDec[denom] = rate.Value
	}

	return ratesDec, nil
}
