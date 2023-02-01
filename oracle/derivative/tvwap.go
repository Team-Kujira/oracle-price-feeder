package derivative

import (
	"time"

	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

type (
	TvwapDerivative struct {
		derivative
		
	}
)

func NewTvwapDerivative(
	pairs map[string]types.CurrencyPair,
	history *history.PriceHistory,
	logger zerolog.Logger,
) (*TvwapDerivative, error) {
	d := &TvwapDerivative{
		derivative: derivative{
			pairs: pairs,
			history: history,
			logger: logger,
		},
	}
	return d, nil
}

func tvwap(
	tickers map[string][]types.TickerPrice,
	start time.Time,
	end time.Time,
) sdk.Dec {
	priceTotal := sdk.ZeroDec()
	volumeTotal := sdk.ZeroDec()
	for _, providerTickers := range tickers {
		providerPriceTotal := sdk.ZeroDec()
		providerVolumeTotal := sdk.ZeroDec()
		providerTimeTotal := int64(0)
		for i, ticker := range providerTickers {
			if ticker.Time.Before(start) {
				continue
			}
			if ticker.Time.After(end) {
				break
			}
			nextIndex := i + 1
			var timeDelta int64
			if nextIndex >= len(providerTickers) || providerTickers[nextIndex].Time.After(end) {
				timeDelta = end.Unix() - ticker.Time.Unix()
			} else {
				timeDelta = providerTickers[nextIndex].Time.Unix() - ticker.Time.Unix()
			}
			providerPriceTotal = providerPriceTotal.Add(ticker.Price.MulInt64(timeDelta))
			providerVolumeTotal = providerVolumeTotal.Add(ticker.Volume.MulInt64(timeDelta))
			providerTimeTotal = providerTimeTotal + timeDelta
		}
		providerWeightedVolume := providerVolumeTotal.QuoInt64(providerTimeTotal)
		providerWeightedPrice := providerPriceTotal.QuoInt64(providerTimeTotal).Mul(providerWeightedVolume)
		priceTotal = priceTotal.Add(providerWeightedPrice)
		volumeTotal = volumeTotal.Add(providerWeightedVolume)
	}
	return priceTotal.Quo(volumeTotal)
}