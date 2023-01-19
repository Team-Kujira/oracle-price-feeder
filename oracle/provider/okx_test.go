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

func TestOkxProvider_GetTickerPrices(t *testing.T) {
	p, err := NewOkxProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testBtcUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]OkxTicker{}
		tickers["ATOM-USDT"] = OkxTicker{
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
		tickers := map[string]OkxTicker{}
		tickers["ATOM-USDT"] = OkxTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
			Time:   "0",
		}

		tickers["BTC-USDT"] = OkxTicker{
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
		require.EqualError(t, err, "okx failed to get ticker price for FOO-BAR")
		require.Nil(t, prices)
	})
}

func TestOkxProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &OkxProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		testBtcUsdtCurrencyPair,
		testAtomUsdtCurrencyPair,
	}
	subMsgs := provider.GetSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(t, `{"op":"subscribe","args":[{"channel":"candle3m","instId":"BTC-USDT"},{"channel":"candle3m","instId":"ATOM-USDT"}]}`, string(msg))
}
