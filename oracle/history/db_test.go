package history

import (
	"testing"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

var (
	testPairAtom = types.CurrencyPair{Base: "ATOM", Quote: "USD"}
	testStartTime1 = time.Unix(0, 0)
	testEndTime1 = time.Unix(10, 0)

	testHistoricalTickers1 = map[string][]types.TickerPrice{
		"osmosis": {
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(0, 0)},
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(1, 0)},
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(2, 0)},
		},
	}
)


func TestPriceHistory_getPrices(t *testing.T) {
	h, err := NewPriceHistory(":memory:", zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, h.Init())
	res1, err1 := h.GetTickerPrices(testPairAtom, testStartTime1, testEndTime1)
	require.NoError(t, err1)
	require.Equal(t, 0, len(res1))
	for provider, tickers := range testHistoricalTickers1 {
		for _, ticker := range tickers {
			err := h.AddTickerPrice(testPairAtom, provider, ticker)
			require.NoError(t, err)
		}
	}
	res2, err2 := h.GetTickerPrices(testPairAtom, testStartTime1, testEndTime1)
	require.NoError(t, err2)
	require.Equal(t, testHistoricalTickers1, res2)
}