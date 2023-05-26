package oracle_test

import (
	"testing"

	"price-feeder/oracle"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"
)

func TestComputeVWAP(t *testing.T) {
	// testCases := map[string]struct {
	// 	prices   []types.TickerPrice
	// 	expected sdk.Dec
	// }{
	// 	"empty prices": {
	// 		prices:   []types.TickerPrice{},
	// 		expected: sdk.Dec{},
	// 	},
	// 	"nil prices": {
	// 		prices:   nil,
	// 		expected: sdk.Dec{},
	// 	},
	// 	"non empty prices": {
	prices := map[string][]types.TickerPrice{}
	prices["ATOM"] = []types.TickerPrice{{
		Price:  sdk.MustNewDecFromStr("28.21000000"),
		Volume: sdk.MustNewDecFromStr("2749102.78000000"),
	}, {
		Price:  sdk.MustNewDecFromStr("28.268700"),
		Volume: sdk.MustNewDecFromStr("178277.53314385"),
	}, {
		Price:  sdk.MustNewDecFromStr("28.168700"),
		Volume: sdk.MustNewDecFromStr("4749102.53314385"),
	}}

	prices["UMEE"] = []types.TickerPrice{{
		Price:  sdk.MustNewDecFromStr("1.13000000"),
		Volume: sdk.MustNewDecFromStr("249102.38000000"),
	}}

	prices["LUNA"] = []types.TickerPrice{{
		Price:  sdk.MustNewDecFromStr("64.87000000"),
		Volume: sdk.MustNewDecFromStr("7854934.69000000"),
	}, {
		Price:  sdk.MustNewDecFromStr("64.87853000"),
		Volume: sdk.MustNewDecFromStr("458917.46353577"),
	}}

	expected := map[string]sdk.Dec{
		"ATOM": sdk.MustNewDecFromStr("28.185812745610043621"),
		"UMEE": sdk.MustNewDecFromStr("1.13000000"),
		"LUNA": sdk.MustNewDecFromStr("64.870470848638112395"),
	}

	for denom, tickers := range prices {
		t.Run(denom, func(t *testing.T) {
			vwap, err := oracle.ComputeVWAP(tickers)
			require.NoError(t, err)
			require.Equal(t, expected[denom], vwap)
		})
	}
}

func TestStandardDeviation(t *testing.T) {
	type result struct {
		mean      sdk.Dec
		deviation sdk.Dec
		err       bool
	}
	restCases := map[string]struct {
		prices   []sdk.Dec
		expected result
	}{
		"empty prices": {
			prices:   []sdk.Dec{},
			expected: result{},
		},
		"nil prices": {
			prices:   nil,
			expected: result{},
		},
		"not enough prices": {
			prices: []sdk.Dec{
				sdk.MustNewDecFromStr("28.21000000"),
				sdk.MustNewDecFromStr("28.23000000"),
			},
			expected: result{},
		},
		"enough prices 1": {
			prices: []sdk.Dec{
				sdk.MustNewDecFromStr("28.21000000"),
				sdk.MustNewDecFromStr("28.23000000"),
				sdk.MustNewDecFromStr("28.40000000"),
			},
			expected: result{
				mean:      sdk.MustNewDecFromStr("28.28"),
				deviation: sdk.MustNewDecFromStr("0.085244745683629475"),
				err:       false,
			},
		},
		"enough prices 2": {
			prices: []sdk.Dec{
				sdk.MustNewDecFromStr("1.13000000"),
				sdk.MustNewDecFromStr("1.13050000"),
				sdk.MustNewDecFromStr("1.14000000"),
			},
			expected: result{
				mean:      sdk.MustNewDecFromStr("1.1335"),
				deviation: sdk.MustNewDecFromStr("0.004600724580614015"),
				err:       false,
			},
		},
	}

	for name, test := range restCases {
		test := test

		t.Run(name, func(t *testing.T) {
			deviation, mean, _ := oracle.StandardDeviation(test.prices)
			// if test.expected.err == false {
			// 	require.NoError(t, err)
			// }
			require.Equal(t, test.expected.deviation, deviation)
			require.Equal(t, test.expected.mean, mean)
		})
	}
}
