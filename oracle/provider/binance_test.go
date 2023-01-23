package provider

import (
	"context"
	"encoding/json"
	"testing"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestBinanceProvider_GetTickerPrices(t *testing.T) {
	p, err := NewBinanceProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		false,
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]BinanceTicker{}
		tickers["ATOMUSDT"] = BinanceTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
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
		tickers := map[string]BinanceTicker{}
		tickers["ATOMUSDT"] = BinanceTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
		}

		tickers["BTCUSDT"] = BinanceTicker{
			Price:  testBtcPriceString,
			Volume: testBtcVolumeString,
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
			testBtcPriceDec,
			prices["BTCUSDT"].Price,
		)
		require.Equal(
			t,
			testBtcVolumeDec,
			prices["BTCUSDT"].Volume,
		)
		require.Equal(
			t,
			testAtomPriceDec,
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			testAtomVolumeDec,
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.EqualError(t, err, "binance failed to get ticker price for FOOBAR")
		require.Nil(t, prices)
	})
}

func TestBinanceProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &BinanceProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		testBtcUsdtCurrencyPair,
		testAtomUsdtCurrencyPair,
	}

	subMsgs := provider.GetSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(t, `{"method":"SUBSCRIBE","params":["btcusdt@kline_1m","atomusdt@kline_1m"],"id":1}`, string(msg))
}
