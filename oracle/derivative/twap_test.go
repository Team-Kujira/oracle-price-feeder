package derivative

import (
	"testing"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"
)

var (
	testHistoricalTickers1 = []types.TickerPrice{
		{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(0, 0)},
		{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(1, 0)},
		{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(2, 0)},
	}
	testTvwapStart1 = time.Unix(0, 0)
	testTvwapEnd1   = time.Unix(3, 0)
	testTvwapPrice1 = sdk.NewDec(5)

	testHistoricalTickers2 = []types.TickerPrice{
		{Price: sdk.NewDec(5), Volume: sdk.NewDec(2), Time: time.Unix(0, 0)},
		{Price: sdk.NewDec(10), Volume: sdk.NewDec(2), Time: time.Unix(1, 0)},
		{Price: sdk.NewDec(15), Volume: sdk.NewDec(2), Time: time.Unix(2, 0)},
		{Price: sdk.NewDec(100), Volume: sdk.NewDec(2), Time: time.Unix(3, 0)},
	}
	testTvwapStart2 = time.Unix(0, 0)
	testTvwapEnd2   = time.Unix(3, 0)
	testTvwapPrice2 = sdk.NewDec(10)

	testHistoricalTickers4 = []types.TickerPrice{
		{Price: sdk.NewDec(5), Volume: sdk.ZeroDec(), Time: time.Unix(2, 0)},
		{Price: sdk.NewDec(8), Volume: sdk.ZeroDec(), Time: time.Unix(4, 0)},
		{Price: sdk.NewDec(2), Volume: sdk.ZeroDec(), Time: time.Unix(6, 0)},
		{Price: sdk.NewDec(7), Volume: sdk.ZeroDec(), Time: time.Unix(10, 0)},
		{Price: sdk.NewDec(4), Volume: sdk.ZeroDec(), Time: time.Unix(12, 0)},
		{Price: sdk.NewDec(5), Volume: sdk.ZeroDec(), Time: time.Unix(14, 0)},
		{Price: sdk.NewDec(1), Volume: sdk.ZeroDec(), Time: time.Unix(16, 0)},
		{Price: sdk.NewDec(3), Volume: sdk.ZeroDec(), Time: time.Unix(18, 0)},
	}
	testTvwapStart4 = time.Unix(3, 0)
	testTvwapEnd4   = time.Unix(17, 0)
	testTvwapPrice4 = sdk.MustNewDecFromStr("4.384615384615384615")

	testHistoricalTickers5 = []types.TickerPrice{
		{Price: sdk.MustNewDecFromStr("1662.000000000000000000"), Volume: sdk.MustNewDecFromStr("92845.781140000000000000"), Time: time.Unix(1675374725, 0)},
		{Price: sdk.MustNewDecFromStr("1662.000000000000000000"), Volume: sdk.MustNewDecFromStr("92845.781140000000000000"), Time: time.Unix(1675374738, 0)},
		{Price: sdk.MustNewDecFromStr("1662.260000000000000000"), Volume: sdk.MustNewDecFromStr("92846.081170000000000000"), Time: time.Unix(1675374762, 0)},
		{Price: sdk.MustNewDecFromStr("1662.390000000000000000"), Volume: sdk.MustNewDecFromStr("92847.739390000000000000"), Time: time.Unix(1675374768, 0)},
		{Price: sdk.MustNewDecFromStr("1662.360000000000000000"), Volume: sdk.MustNewDecFromStr("92813.937650000000000000"), Time: time.Unix(1675374786, 0)},
		{Price: sdk.MustNewDecFromStr("1662.510000000000000000"), Volume: sdk.MustNewDecFromStr("92832.244300000000000000"), Time: time.Unix(1675374793, 0)},
		{Price: sdk.MustNewDecFromStr("1659.900000000000000000"), Volume: sdk.MustNewDecFromStr("92858.117110000000000000"), Time: time.Unix(1675374854, 0)},
		{Price: sdk.MustNewDecFromStr("1659.430000000000000000"), Volume: sdk.MustNewDecFromStr("92879.370440000000000000"), Time: time.Unix(1675374860, 0)},
		{Price: sdk.MustNewDecFromStr("1656.520000000000000000"), Volume: sdk.MustNewDecFromStr("92941.952340000000000000"), Time: time.Unix(1675374885, 0)},
		{Price: sdk.MustNewDecFromStr("1657.560000000000000000"), Volume: sdk.MustNewDecFromStr("92945.846780000000000000"), Time: time.Unix(1675374891, 0)},
		{Price: sdk.MustNewDecFromStr("1656.600000000000000000"), Volume: sdk.MustNewDecFromStr("92947.210290000000000000"), Time: time.Unix(1675374909, 0)},
		{Price: sdk.MustNewDecFromStr("1655.450000000000000000"), Volume: sdk.MustNewDecFromStr("92951.304370000000000000"), Time: time.Unix(1675374921, 0)},
		{Price: sdk.MustNewDecFromStr("1656.230000000000000000"), Volume: sdk.MustNewDecFromStr("92978.975810000000000000"), Time: time.Unix(1675374940, 0)},
		{Price: sdk.MustNewDecFromStr("1658.270000000000000000"), Volume: sdk.MustNewDecFromStr("92992.652850000000000000"), Time: time.Unix(1675374958, 0)},
		{Price: sdk.MustNewDecFromStr("1656.730000000000000000"), Volume: sdk.MustNewDecFromStr("92944.557140000000000000"), Time: time.Unix(1675374976, 0)},
		{Price: sdk.MustNewDecFromStr("1655.510000000000000000"), Volume: sdk.MustNewDecFromStr("92950.435240000000000000"), Time: time.Unix(1675374982, 0)},
		{Price: sdk.MustNewDecFromStr("1654.250000000000000000"), Volume: sdk.MustNewDecFromStr("92974.853870000000000000"), Time: time.Unix(1675375007, 0)},
		{Price: sdk.MustNewDecFromStr("1652.640000000000000000"), Volume: sdk.MustNewDecFromStr("92984.726620000000000000"), Time: time.Unix(1675375019, 0)},
		{Price: sdk.MustNewDecFromStr("1654.320000000000000000"), Volume: sdk.MustNewDecFromStr("92972.304940000000000000"), Time: time.Unix(1675375037, 0)},
		{Price: sdk.MustNewDecFromStr("1655.540000000000000000"), Volume: sdk.MustNewDecFromStr("92981.268120000000000000"), Time: time.Unix(1675375049, 0)},
		{Price: sdk.MustNewDecFromStr("1655.630000000000000000"), Volume: sdk.MustNewDecFromStr("93004.087100000000000000"), Time: time.Unix(1675375074, 0)},
		{Price: sdk.MustNewDecFromStr("1654.930000000000000000"), Volume: sdk.MustNewDecFromStr("92954.475200000000000000"), Time: time.Unix(1675375080, 0)},
		{Price: sdk.MustNewDecFromStr("1656.130000000000000000"), Volume: sdk.MustNewDecFromStr("92974.349730000000000000"), Time: time.Unix(1675375098, 0)},
		{Price: sdk.MustNewDecFromStr("1654.480000000000000000"), Volume: sdk.MustNewDecFromStr("92996.241820000000000000"), Time: time.Unix(1675375110, 0)},
		{Price: sdk.MustNewDecFromStr("1652.950000000000000000"), Volume: sdk.MustNewDecFromStr("93002.265750000000000000"), Time: time.Unix(1675375129, 0)},
		{Price: sdk.MustNewDecFromStr("1651.720000000000000000"), Volume: sdk.MustNewDecFromStr("92987.956590000000000000"), Time: time.Unix(1675375141, 0)},
	}
	testTvwapStart5 = time.Unix(1675374700, 0)
	testTvwapEnd5   = time.Unix(1675375150, 0)
	testTvwapPrice5 = sdk.MustNewDecFromStr("1657.772658823529411764")

	testHistoricalTickers6 = []types.TickerPrice{
		{Price: sdk.NewDec(8), Volume: sdk.ZeroDec(), Time: time.Unix(0, 0)},
		{Price: sdk.NewDec(12), Volume: sdk.ZeroDec(), Time: time.Unix(3, 0)},
		{Price: sdk.NewDec(11), Volume: sdk.ZeroDec(), Time: time.Unix(6, 0)},
		{Price: sdk.NewDec(9), Volume: sdk.ZeroDec(), Time: time.Unix(9, 0)},
	}
	testTvwapStart6 = time.Unix(0, 0)
	testTvwapEnd6   = time.Unix(12, 0)
	testTvwapPrice6 = sdk.MustNewDecFromStr("10")
)

func TestTvwapDerivative_tvwap(t *testing.T) {
	result1, _, err := Twap(testHistoricalTickers1, testTvwapStart1, testTvwapEnd1)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice1, result1)

	result2, _, err := Twap(testHistoricalTickers2, testTvwapStart2, testTvwapEnd2)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice2, result2)

	result4, _, err := Twap(testHistoricalTickers4, testTvwapStart4, testTvwapEnd4)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice4, result4)

	result5, _, err := Twap(testHistoricalTickers5, testTvwapStart5, testTvwapEnd5)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice5, result5)

	result6, _, err := Twap(testHistoricalTickers6, testTvwapStart6, testTvwapEnd6)
	require.NoError(t, err)
	require.Equal(t, testTvwapPrice6, result6)
}
