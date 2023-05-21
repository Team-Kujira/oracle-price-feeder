package oracle

import (
	"fmt"
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

// StandardDeviation returns standard deviation and mean of assets.
// Will skip calculating for an asset if there are less than 3 prices.
func StandardDeviation(prices []sdk.Dec) (sdk.Dec, sdk.Dec, error) {
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
