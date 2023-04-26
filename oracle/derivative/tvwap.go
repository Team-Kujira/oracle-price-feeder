package derivative

import (
	"fmt"
	"time"

	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

const (
	tvwapMaxTimeDeltaSeconds      = int64(120)
	tvwapMinHistoryPeriodFraction = 0.8
)

type (
	TvwapDerivative struct {
		derivative
	}
)

func NewTvwapDerivative(
	history *history.PriceHistory,
	logger zerolog.Logger,
	pairs []types.CurrencyPair,
	periods map[string]time.Duration,
) (*TvwapDerivative, error) {
	d := &TvwapDerivative{
		derivative: derivative{
			pairs:   pairs,
			history: history,
			logger:  logger,
			periods: periods,
		},
	}
	return d, nil
}

func (d *TvwapDerivative) GetPrices(pairs ...types.CurrencyPair) (map[string]sdk.Dec, error) {
	prices := make(map[string]sdk.Dec, len(pairs))
	now := time.Now()
	for _, pair := range pairs {
		period, ok := d.periods[pair.String()]
		if !ok {
			d.logger.Error().Str("pair", pair.String()).Msg("pair not configured")
			return nil, fmt.Errorf("pair not configured")
		}
		start := now.Add(-period)
		tickers, err := d.history.GetTickerPrices(pair, start, now)
		if err != nil {
			d.logger.Error().Err(err).Str("pair", pair.String()).Msg("failed to get historical tickers")
			return nil, err
		}
		pairPrices, err := tvwap(tickers, start, now)
		if err != nil {
			d.logger.Warn().Err(err).Str("pair", pair.String()).Dur("period", period).Msg("failed to compute derivative price")
		} else {
			prices[pair.String()] = pairPrices
		}
	}
	return prices, nil
}

func tvwap(
	tickers map[string][]types.TickerPrice,
	start time.Time,
	end time.Time,
) (sdk.Dec, error) {
	priceTotal := sdk.ZeroDec()
	volumeTotal := sdk.ZeroDec()
	period := end.Sub(start).Seconds()
	minPeriod := int64(tvwapMinHistoryPeriodFraction * period)
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
			if timeDelta > tvwapMaxTimeDeltaSeconds {
				return sdk.Dec{}, fmt.Errorf("missing history for pair")
			}
			providerPriceTotal = providerPriceTotal.Add(ticker.Price.MulInt64(timeDelta))
			providerVolumeTotal = providerVolumeTotal.Add(ticker.Volume.MulInt64(timeDelta))
			providerTimeTotal = providerTimeTotal + timeDelta
		}
		if providerTimeTotal == 0 || providerTimeTotal < minPeriod {
			continue
		}
		providerWeightedVolume := providerVolumeTotal.QuoInt64(providerTimeTotal)
		providerWeightedPrice := providerPriceTotal.QuoInt64(providerTimeTotal).Mul(providerWeightedVolume)
		priceTotal = priceTotal.Add(providerWeightedPrice)
		volumeTotal = volumeTotal.Add(providerWeightedVolume)
	}
	if volumeTotal.IsZero() {
		return sdk.Dec{}, fmt.Errorf("no volume for pair or not enough history")
	}
	return priceTotal.Quo(volumeTotal), nil
}

func Tvwap(
	tickers map[string][]types.TickerPrice,
	start time.Time,
	end time.Time,
) (sdk.Dec, error) {
	return tvwap(tickers, start, end)
}
