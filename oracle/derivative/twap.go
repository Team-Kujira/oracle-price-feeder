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
	twapMaxTimeDeltaSeconds      = int64(120)
	twapMinHistoryPeriodFraction = 0.8
)

type (
	TwapDerivative struct {
		derivative
	}
)

func NewTwapDerivative(
	history *history.PriceHistory,
	logger zerolog.Logger,
	pairs []types.CurrencyPair,
	periods map[string]time.Duration,
) (*TwapDerivative, error) {
	d := &TwapDerivative{
		derivative: derivative{
			pairs:   pairs,
			history: history,
			logger:  logger,
			periods: periods,
		},
	}
	return d, nil
}

func (d *TwapDerivative) GetPrices(symbol string) (map[string]types.TickerPrice, error) {
	now := time.Now()

	period, ok := d.periods[symbol]
	if !ok {
		d.logger.Error().
			Str("symbol", symbol).
			Msg("pair not configured")
		return nil, fmt.Errorf("pair not configured")
	}

	start := now.Add(-period)
	tickers, err := d.history.GetTickerPrices(symbol, start, now)
	if err != nil {
		d.logger.Error().
			Err(err).
			Str("symbol", symbol).
			Msg("failed to get historical tickers")
		return nil, err
	}

	derivativePrices := map[string]types.TickerPrice{}
	for providerName, tickerPrices := range tickers {

		pairPrice, missing, err := Twap(tickerPrices, start, now)
		if err != nil || pairPrice.IsNil() || pairPrice.IsZero() {
			d.logger.Warn().
				Err(err).
				Str("symbol", symbol).
				Str("period", period.String()).
				Str("missing", (time.Second * time.Duration(missing)).String()).
				Msg("failed to compute derivative price")
			continue
		}

		latestTicker := tickerPrices[len(tickerPrices)-1]

		derivativePrices[providerName] = types.TickerPrice{
			Price:  pairPrice,
			Volume: latestTicker.Volume,
			Time:   now,
		}
	}

	return derivativePrices, nil
}

func Twap(
	tickers []types.TickerPrice,
	start time.Time,
	end time.Time,
) (sdk.Dec, int64, error) {
	priceTotal := sdk.ZeroDec()
	volumeTotal := sdk.ZeroDec()
	timeTotal := int64(0)

	period := end.Sub(start).Seconds()
	minPeriod := int64(twapMinHistoryPeriodFraction * period)

	var newStart time.Time

	for i, ticker := range tickers {
		if ticker.Time.Before(start) {
			continue
		}
		if ticker.Time.After(end) {
			break
		}
		nextIndex := i + 1
		var timeDelta int64
		if nextIndex >= len(tickers) || tickers[nextIndex].Time.After(end) {
			timeDelta = end.Unix() - ticker.Time.Unix()
		} else {
			timeDelta = tickers[nextIndex].Time.Unix() - ticker.Time.Unix()
		}

		if timeDelta > twapMaxTimeDeltaSeconds {
			if nextIndex >= len(tickers) {
				newStart = end
			} else {
				newStart = tickers[nextIndex].Time
			}
		}

		priceTotal = priceTotal.Add(ticker.Price.MulInt64(timeDelta))
		volumeTotal = volumeTotal.Add(ticker.Volume)
		timeTotal = timeTotal + timeDelta
	}

	if !newStart.IsZero() {
		missing := newStart.Unix() - time.Now().Unix() + int64(period)
		return sdk.Dec{}, missing, fmt.Errorf("not enough continuous history")
	}

	if timeTotal == 0 || timeTotal < minPeriod {
		missing := minPeriod - timeTotal
		return sdk.Dec{}, missing, fmt.Errorf("not enough history")
	}

	return priceTotal.QuoInt64(timeTotal), 0, nil
}
