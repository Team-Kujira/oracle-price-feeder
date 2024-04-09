package derivative

import (
	"fmt"
	"sort"
	"time"

	"price-feeder/oracle/history"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

const (
	twapMaxTimeDeltaSeconds      = int64(120)
	twapMinHistoryPeriodFraction = 0.8
	twapMaxPriceDeviation        = 0.05
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
				Str("provider", providerName).
				Str("period", period.String()).
				Str("missing", (time.Second * time.Duration(missing)).String()).
				Msg("failed to compute twap")
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
	timeTotal := int64(0)

	period := end.Sub(start).Seconds()
	minPeriod := int64(twapMinHistoryPeriodFraction * period)

	discardedTime := int64(0)

	threshold := sdk.MustNewDecFromStr(fmt.Sprintf("%f", twapMaxPriceDeviation))

	filtered := []types.TickerPrice{}
	for _, ticker := range tickers {
		if ticker.Time.Before(start) {
			continue
		}
		if ticker.Time.After(end) {
			break
		}

		filtered = append(filtered, ticker)
	}

	tickers = filtered

	median, err := weightedMedian(tickers)
	if err != nil {
		return sdk.Dec{}, 0, err
	}

	if median.IsZero() {
		return sdk.Dec{}, 0, fmt.Errorf("median is 0")
	}

	for i, ticker := range tickers {

		nextIndex := i + 1
		if nextIndex >= len(tickers) || tickers[nextIndex].Time.After(end) {
			break
		}

		timeDelta := tickers[nextIndex].Time.Unix() - ticker.Time.Unix()

		if timeDelta > twapMaxTimeDeltaSeconds {
			discardedTime = discardedTime + timeDelta
			continue
		}

		// check if price has a big spike
		max := median.Add(median.Mul(threshold))
		min := median.Sub(median.Mul(threshold))

		if ticker.Price.GT(max) || ticker.Price.LT(min) {
			// that's too much, ignore
			continue
		}

		priceTotal = priceTotal.Add(ticker.Price.MulInt64(timeDelta))
		timeTotal = timeTotal + timeDelta
	}

	if timeTotal < minPeriod {
		missing := minPeriod - timeTotal
		message := "not enough history"

		if int64(period)-discardedTime < minPeriod {
			message = "too much time gap in history"
		}

		return sdk.Dec{}, missing, fmt.Errorf(message)
	}

	return priceTotal.QuoInt64(timeTotal), 0, nil
}

func weightedMedian(tickers []types.TickerPrice) (sdk.Dec, error) {
	type Price struct {
		Price  sdk.Dec
		Weight int64
	}

	if len(tickers) < 2 {
		return sdk.ZeroDec(), nil
	}

	prices := []Price{}

	for i := 1; i < len(tickers); i++ {
		weight := tickers[i].Time.Unix() - tickers[i-1].Time.Unix()
		prices = append(prices, Price{
			Price:  tickers[i].Price,
			Weight: weight,
		})
	}

	sort.Slice(prices, func(i, j int) bool {
		return prices[i].Price.LT(prices[j].Price)
	})

	// weighted median
	total := tickers[len(tickers)-1].Time.Unix() - tickers[0].Time.Unix()
	pivot := 0

	for _, price := range prices {
		pivot += int(price.Weight)
		if pivot > int(total/2) {
			return price.Price, nil
		}
	}

	return sdk.ZeroDec(), nil
}
