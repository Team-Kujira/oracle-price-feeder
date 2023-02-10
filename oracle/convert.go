package oracle

import (
	"fmt"
	"strings"

	"price-feeder/config"
	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

// getUSDBasedProviders retrieves which providers for an asset have a USD-based pair,
// given the asset and the map of providers to currency pairs.
func getUSDBasedProviders(
	asset string,
	providerPairs map[provider.Name][]types.CurrencyPair,
) (map[provider.Name]struct{}, error) {
	conversionProviders := make(map[provider.Name]struct{})

	for provider, pairs := range providerPairs {
		for _, pair := range pairs {
			if strings.ToUpper(pair.Quote) == config.DenomUSD && strings.ToUpper(pair.Base) == asset {
				conversionProviders[provider] = struct{}{}
			}
		}
	}
	if len(conversionProviders) == 0 {
		return nil, fmt.Errorf("no providers have a usd conversion for this asset")
	}

	return conversionProviders, nil
}

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
) (provider.AggregatedProviderPrices, error) {
	if len(tickers) == 0 {
		return tickers, nil
	}

	conversionRates := make(map[string]sdk.Dec)
	requiredConversions := make(map[provider.Name][]types.CurrencyPair)

	for pairProviderName, pairs := range providerPairs {
		for _, pair := range pairs {
			if strings.ToUpper(pair.Quote) != config.DenomUSD {
				// Get valid providers and use them to generate a USD-based price for this asset.
				validProviders, err := getUSDBasedProviders(pair.Quote, providerPairs)
				if err != nil {
					return nil, err
				}

				// Find valid candles, and then let's re-compute the tvwap.
				validTickerList := provider.AggregatedProviderPrices{}
				for providerName, candleSet := range tickers {
					// Find tickers which we can use for conversion, and calculate the vwap
					// to find the conversion rate.
					if _, ok := validProviders[providerName]; ok {
						for base, ticker := range candleSet {
							if base == pair.Quote {
								if _, ok := validTickerList[providerName]; !ok {
									validTickerList[providerName] = make(map[string]types.TickerPrice)
								}

								validTickerList[providerName][base] = ticker
							}
						}
					}
				}

				if len(validTickerList) == 0 {
					return nil, fmt.Errorf("there are no valid conversion rates for %s", pair.Quote)
				}

				filteredTickers, err := FilterTickerDeviations(
					logger,
					validTickerList,
					deviationThresholds,
				)
				if err != nil {
					return nil, err
				}

				vwap, err := ComputeVWAP(filteredTickers)
				if err != nil {
					return nil, err
				}

				conversionRates[pair.Quote] = vwap[pair.Quote]
				requiredConversions[pairProviderName] = append(
					requiredConversions[pairProviderName], pair,
				)
			}
		}
	}

	// Convert assets to USD.
	for providerName, tickerPrices := range tickers {
		for base := range tickerPrices {
			for _, currencyPair := range requiredConversions[providerName] {
				if currencyPair.Base == base {
					if conversionRates[currencyPair.Quote].IsNil() {
						logger.Error().
							Str("denom", currencyPair.Quote).
							Msg("missing conversion rate")
						continue
					}
					tickerPrices[base] = types.TickerPrice{
						Price: tickerPrices[base].Price.Mul(
							conversionRates[currencyPair.Quote],
						),
						Volume: tickerPrices[base].Volume,
						Time:   tickerPrices[base].Time,
					}
				}
			}
		}
	}

	return tickers, nil
}
