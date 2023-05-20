package oracle

import (
	"fmt"
	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// ComputeVWAP computes the volume weighted average price for all tickers
// of all pairs of the same symbol.
// Ref: https://en.wikipedia.org/wiki/Volume-weighted_average_price
func ComputeVWAP(tickers []types.TickerPrice) (sdk.Dec, error) {
	weightedPrice := sdk.ZeroDec()
	volumeSum := sdk.ZeroDec()

	for _, tp := range tickers {
		// weightedPrice = Σ {P * V} for all TickerPrice
		weightedPrice = weightedPrice.Add(tp.Price.Mul(tp.Volume))

		// track total volume for each base
		volumeSum = volumeSum.Add(tp.Volume)
	}

	if volumeSum.Equal(sdk.ZeroDec()) {
		return sdk.Dec{}, fmt.Errorf("total volume is zero")
	}

	return weightedPrice.Quo(volumeSum), nil
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

func StandardDeviation2(prices []sdk.Dec) (sdk.Dec, sdk.Dec, error) {
	// Skip if standard deviation would not be meaningful
	if len(prices) < 3 {
		err := fmt.Errorf("not enough values to calculate deviation")
		return sdk.Dec{}, sdk.Dec{}, err
	}

	sum := sdk.ZeroDec()

	for _, price := range prices {
		sum = sum.Add(price)
	}

	numPrices := int64(len(prices))
	mean := sum.QuoInt64(numPrices)
	varianceSum := sdk.ZeroDec()

	for _, price := range prices {
		deviation := price.Sub(mean)
		varianceSum = varianceSum.Add(deviation.Mul(deviation))
	}

	variance := varianceSum.QuoInt64(numPrices)

	deviation, err := variance.ApproxSqrt()
	if err != nil {
		return sdk.Dec{}, sdk.Dec{}, err
	}

	return deviation, mean, nil
}
