package provider

import (
	"testing"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"
)

var (
	ping = []byte("ping")

	// vars to be used in the provider specific tests
	testAtomUsdtCurrencyPair = types.CurrencyPair{
		Base:  "ATOM",
		Quote: "USDT",
	}
	testAtomPriceFloat64  = float64(12.3456)
	testAtomPriceString   = "12.3456"
	testAtomPriceInt64    = int64(1234560000)
	testAtomPriceDec      = sdk.NewDec(1234560000).QuoInt64(100000000)
	testAtomVolumeFloat64 = float64(7654321.98765)
	testAtomVolumeString  = "7654321.98765"
	testAtomVolumeInt64   = int64(765432198765000)
	testAtomVolumeDec     = sdk.NewDec(765432198765000).QuoInt64(100000000)
	testAtomTicker        = types.TickerPrice{
		Price:  testAtomPriceDec,
		Volume: testAtomVolumeDec,
		Time:   time.Now(),
	}

	testBtcUsdtCurrencyPair = types.CurrencyPair{
		Base:  "BTC",
		Quote: "USDT",
	}
	testBtcPriceFloat64  = float64(12345.6789)
	testBtcPriceString   = "12345.6789"
	testBtcPriceInt64    = int64(1234567890000)
	testBtcPriceDec      = sdk.NewDec(1234567890000).QuoInt64(100000000)
	testBtcVolumeFloat64 = float64(7654.32198765)
	testBtcVolumeString  = "7654.32198765"
	testBtcVolumeInt64   = int64(765432198765)
	testBtcVolumeDec     = sdk.NewDec(765432198765).QuoInt64(100000000)
	testBtcTicker        = types.TickerPrice{
		Price:  testBtcPriceDec,
		Volume: testBtcVolumeDec,
		Time:   time.Now(),
	}

	testFooBarCurrencyPair = types.CurrencyPair{
		Base:  "FOO",
		Quote: "BAR",
	}

	testTickersAtom    = map[string]types.TickerPrice{"ATOMUSDT": testAtomTicker}
	testTickersAtomBtc = map[string]types.TickerPrice{
		"ATOMUSDT": testAtomTicker,
		"BTCUSDT":  testBtcTicker,
	}
)

func TestStrToDec(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		dec := strToDec("3.3")
		require.Equal(t, sdk.MustNewDecFromStr("3.3"), dec)
	})

	t.Run("long_precision", func(t *testing.T) {
		dec := strToDec("3.323454654756344465786786524")
		exp, _ := sdk.NewDecFromStr("3.323454654756344465")

		require.Equal(t, exp, dec)
	})

	t.Run("non_number", func(t *testing.T) {
		dec := strToDec("x")
		require.Equal(t, sdk.Dec{}, dec)
	})

	t.Run("empty_string", func(t *testing.T) {
		dec := strToDec("")
		require.Equal(t, sdk.Dec{}, dec)
	})
}
