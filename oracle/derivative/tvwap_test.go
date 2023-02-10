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

	testHistoricalTickers4 = map[string][]types.TickerPrice{
		"osmosis": {
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(0, 0)},
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(200, 0)},
			types.TickerPrice{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(400, 0)},
		},
	}
	testTvwapStart4 = time.Unix(0, 0)
	testTvwapEnd4 = time.Unix(200, 0)

	testHistoricalTickers5 = map[string][]types.TickerPrice{
		"binanceus": {
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1662.000000000000000000"), Volume: sdk.MustNewDecFromStr("92845.781140000000000000"), Time: time.Unix(1675374725, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1662.000000000000000000"), Volume: sdk.MustNewDecFromStr("92845.781140000000000000"), Time: time.Unix(1675374738, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1662.260000000000000000"), Volume: sdk.MustNewDecFromStr("92846.081170000000000000"), Time: time.Unix(1675374762, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1662.390000000000000000"), Volume: sdk.MustNewDecFromStr("92847.739390000000000000"), Time: time.Unix(1675374768, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1662.360000000000000000"), Volume: sdk.MustNewDecFromStr("92813.937650000000000000"), Time: time.Unix(1675374786, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1662.510000000000000000"), Volume: sdk.MustNewDecFromStr("92832.244300000000000000"), Time: time.Unix(1675374793, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1659.900000000000000000"), Volume: sdk.MustNewDecFromStr("92858.117110000000000000"), Time: time.Unix(1675374854, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1659.430000000000000000"), Volume: sdk.MustNewDecFromStr("92879.370440000000000000"), Time: time.Unix(1675374860, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1656.520000000000000000"), Volume: sdk.MustNewDecFromStr("92941.952340000000000000"), Time: time.Unix(1675374885, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1657.560000000000000000"), Volume: sdk.MustNewDecFromStr("92945.846780000000000000"), Time: time.Unix(1675374891, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1656.600000000000000000"), Volume: sdk.MustNewDecFromStr("92947.210290000000000000"), Time: time.Unix(1675374909, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1655.450000000000000000"), Volume: sdk.MustNewDecFromStr("92951.304370000000000000"), Time: time.Unix(1675374921, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1656.230000000000000000"), Volume: sdk.MustNewDecFromStr("92978.975810000000000000"), Time: time.Unix(1675374940, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1658.270000000000000000"), Volume: sdk.MustNewDecFromStr("92992.652850000000000000"), Time: time.Unix(1675374958, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1656.730000000000000000"), Volume: sdk.MustNewDecFromStr("92944.557140000000000000"), Time: time.Unix(1675374976, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1655.510000000000000000"), Volume: sdk.MustNewDecFromStr("92950.435240000000000000"), Time: time.Unix(1675374982, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1654.250000000000000000"), Volume: sdk.MustNewDecFromStr("92974.853870000000000000"), Time: time.Unix(1675375007, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1652.640000000000000000"), Volume: sdk.MustNewDecFromStr("92984.726620000000000000"), Time: time.Unix(1675375019, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1654.320000000000000000"), Volume: sdk.MustNewDecFromStr("92972.304940000000000000"), Time: time.Unix(1675375037, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1655.540000000000000000"), Volume: sdk.MustNewDecFromStr("92981.268120000000000000"), Time: time.Unix(1675375049, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1655.630000000000000000"), Volume: sdk.MustNewDecFromStr("93004.087100000000000000"), Time: time.Unix(1675375074, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1654.930000000000000000"), Volume: sdk.MustNewDecFromStr("92954.475200000000000000"), Time: time.Unix(1675375080, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1656.130000000000000000"), Volume: sdk.MustNewDecFromStr("92974.349730000000000000"), Time: time.Unix(1675375098, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1654.480000000000000000"), Volume: sdk.MustNewDecFromStr("92996.241820000000000000"), Time: time.Unix(1675375110, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1652.950000000000000000"), Volume: sdk.MustNewDecFromStr("93002.265750000000000000"), Time: time.Unix(1675375129, 0)},
			types.TickerPrice{Price: sdk.MustNewDecFromStr("1651.720000000000000000"), Volume: sdk.MustNewDecFromStr("92987.956590000000000000"), Time: time.Unix(1675375141, 0)},
		},
	}
	testTvwapStart5 = time.Unix(1675374700, 0)
	testTvwapEnd5 = time.Unix(1675375150, 0)
	testTvwapPrice5 = sdk.MustNewDecFromStr("1657.772658823529411764")
)

func TestTvwapDerivative_tvwap(t *testing.T) {
	result1, err := tvwap(testHistoricalTickers1, testTvwapStart1, testTvwapEnd1)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice1, result1)
	result2, err := tvwap(testHistoricalTickers2, testTvwapStart2, testTvwapEnd2)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice2, result2)
	result3, err := tvwap(testHistoricalTickers3, testTvwapStart3, testTvwapEnd3)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice3, result3)
	_, err = tvwap(testHistoricalTickers4, testTvwapStart4, testTvwapEnd4)
	require.Error(t, err)
	result5, err := tvwap(testHistoricalTickers5, testTvwapStart5, testTvwapEnd5)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice5, result5)
}