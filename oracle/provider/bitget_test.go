package provider

import (
	"context"
	"testing"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestBitgetProvider_GetTickerPrices(t *testing.T) {
	p, err := NewBitgetProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testBtcUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]BitgetTicker{}
		tickers["ATOMUSDT"] = BitgetTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
			Time:   "0",
		}

		p.tickers = tickers

		prices, err := p.GetTickerPrices(testAtomUsdtCurrencyPair)
		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testAtomPriceString),
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testAtomVolumeString),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		tickers := map[string]BitgetTicker{}
		tickers["ATOMUSDT"] = BitgetTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
			Time:   "0",
		}

		tickers["BTCUSDT"] = BitgetTicker{
			Price:  testBtcPriceString,
			Volume: testBtcVolumeString,
			Time:   "0",
		}

		p.tickers = tickers
		prices, err := p.GetTickerPrices(
			testAtomUsdtCurrencyPair,
			testBtcUsdtCurrencyPair,
		)

		require.NoError(t, err)
		require.Len(t, prices, 2)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testBtcPriceString),
			prices["BTCUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testBtcVolumeString),
			prices["BTCUSDT"].Volume,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testAtomPriceString),
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testAtomVolumeString),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.EqualError(t, err, "bitget failed to get ticker price for FOOBAR")
		require.Nil(t, prices)
	})
}
