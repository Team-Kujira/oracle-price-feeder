package oracle

import (
	"price-feeder/oracle/provider"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// ComputeVWAP computes the volume weighted average price for all price points
// for each ticker/exchange pair. The provided prices argument reflects a mapping
// of provider => {<base> => <TickerPrice>, ...}.
//
// Ref: https://en.wikipedia.org/wiki/Volume-weighted_average_price
func ComputeVWAP(prices provider.AggregatedProviderPrices) (map[string]sdk.Dec, error) {
	var (
		weightedPrices = make(map[string]sdk.Dec)
		volumeSum      = make(map[string]sdk.Dec)
	)

	for _, providerPrices := range prices {
		for base, tp := range providerPrices {
			if _, ok := weightedPrices[base]; !ok {
				weightedPrices[base] = sdk.ZeroDec()
			}
			if _, ok := volumeSum[base]; !ok {
				volumeSum[base] = sdk.ZeroDec()
			}

			// weightedPrices[base] = Σ {P * V} for all TickerPrice
			weightedPrices[base] = weightedPrices[base].Add(tp.Price.Mul(tp.Volume))

			// track total volume for each base
			volumeSum[base] = volumeSum[base].Add(tp.Volume)
		}
	}

	vwap := make(map[string]sdk.Dec)

	for base, price := range weightedPrices {
		if !volumeSum[base].Equal(sdk.ZeroDec()) {
			if _, ok := vwap[base]; !ok {
				vwap[base] = sdk.ZeroDec()
			}

			vwap[base] = price.Quo(volumeSum[base])
		}
	}

	return vwap, nil
}

// StandardDeviation returns maps of the standard deviations and means of assets.
// Will skip calculating for an asset if there are less than 3 prices.
func StandardDeviation(
	prices map[provider.Name]map[string]sdk.Dec,
) (map[string]sdk.Dec, map[string]sdk.Dec, error) {
	var (
		deviations = make(map[string]sdk.Dec)
		means      = make(map[string]sdk.Dec)
		priceSlice = make(map[string][]sdk.Dec)
		priceSums  = make(map[string]sdk.Dec)
	)

	for _, providerPrices := range prices {
		for base, p := range providerPrices {
			if _, ok := priceSums[base]; !ok {
				priceSums[base] = sdk.ZeroDec()
			}
			if _, ok := priceSlice[base]; !ok {
				priceSlice[base] = []sdk.Dec{}
			}

			priceSums[base] = priceSums[base].Add(p)
			priceSlice[base] = append(priceSlice[base], p)
		}
	}

	for base, sum := range priceSums {
		// Skip if standard deviation would not be meaningful
		if len(priceSlice[base]) < 3 {
			continue
		}
		if _, ok := deviations[base]; !ok {
			deviations[base] = sdk.ZeroDec()
		}
		if _, ok := means[base]; !ok {
			means[base] = sdk.ZeroDec()
		}

		numPrices := int64(len(priceSlice[base]))
		means[base] = sum.QuoInt64(numPrices)
		varianceSum := sdk.ZeroDec()

		for _, price := range priceSlice[base] {
			deviation := price.Sub(means[base])
			varianceSum = varianceSum.Add(deviation.Mul(deviation))
		}

		variance := varianceSum.QuoInt64(numPrices)

		standardDeviation, err := variance.ApproxSqrt()
		if err != nil {
			return make(map[string]sdk.Dec), make(map[string]sdk.Dec), err
		}

		deviations[base] = standardDeviation
	}

	return deviations, means, nil
}
