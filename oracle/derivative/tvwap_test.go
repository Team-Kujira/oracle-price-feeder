package derivative

import (
	"testing"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"
)


var (
	testHistoricalTickers1 = map[string][]types.TickerPrice{
		"osmosis": {
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(0, 0)},
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(1, 0)},
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(2, 0)},
		},
	}
	testTvwapStart1 = time.Unix(0, 0)
	testTvwapEnd1 = time.Unix(3, 0)
	testTvwapPrice1 = sdk.NewDec(5)

	testHistoricalTickers2 = map[string][]types.TickerPrice{
		"osmosis": {
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(0, 0)},
			types.TickerPrice{Price: sdk.NewDec(10), Volume: sdk.NewDec(2), Time: time.Unix(1, 0)},
			types.TickerPrice{Price: sdk.NewDec(15), Volume: sdk.NewDec(2), Time: time.Unix(2, 0)},
			types.TickerPrice{Price: sdk.NewDec(100), Volume: sdk.NewDec(2), Time: time.Unix(3, 0)},
		},
	}
	testTvwapStart2 = time.Unix(0, 0)
	testTvwapEnd2 = time.Unix(3, 0)
	testTvwapPrice2 = sdk.NewDec(10)

	testHistoricalTickers3 = map[string][]types.TickerPrice{
		"osmosis": {
			types.TickerPrice{Price: sdk.NewDec(8), Volume: sdk.NewDec(1000), Time: time.Unix(0, 0)},
			types.TickerPrice{Price: sdk.NewDec(12), Volume: sdk.NewDec(1000), Time: time.Unix(3, 0)},
			types.TickerPrice{Price: sdk.NewDec(11), Volume: sdk.NewDec(1000), Time: time.Unix(6, 0)},
			types.TickerPrice{Price: sdk.NewDec(9), Volume: sdk.NewDec(1000), Time: time.Unix(9, 0)},
		},
		"binance": {
			types.TickerPrice{Price: sdk.NewDec(100), Volume: sdk.NewDec(10), Time: time.Unix(3, 0)},
			types.TickerPrice{Price: sdk.NewDec(110), Volume: sdk.NewDec(10), Time: time.Unix(6, 0)},
			types.TickerPrice{Price: sdk.NewDec(90), Volume: sdk.NewDec(10), Time: time.Unix(9, 0)},
		},
	}
	testTvwapStart3 = time.Unix(0, 0)
	testTvwapEnd3 = time.Unix(12, 0)
	testTvwapPrice3 = sdk.MustNewDecFromStr("10.891089108910891089")
)

func TestTvwapDerivative_tvwap(t *testing.T) {
	result1 := tvwap(testHistoricalTickers1, testTvwapStart1, testTvwapEnd1)
	require.Equal(t, testTvwapPrice1, result1)
	result2 := tvwap(testHistoricalTickers2, testTvwapStart2, testTvwapEnd2)
	require.Equal(t, testTvwapPrice2, result2)
	result3 := tvwap(testHistoricalTickers3, testTvwapStart3, testTvwapEnd3)
	require.Equal(t, testTvwapPrice3, result3)
}