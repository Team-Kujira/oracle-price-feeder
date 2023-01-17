package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMexcProvider_GetTickerPrices(t *testing.T) {
	p, err := NewMexcProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		types.CurrencyPair{Base: "ATOM", Quote: "USDT"},
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickerMap := map[string]MexcTicker{}
		tickerMap["ATOMUSDT"] = MexcTicker{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeString,
		}

		p.tickers = tickerMap

		prices, err := p.GetTickerPrices(testAtomUsdtCurrencyPair)
		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testAtomPriceFloat64)),
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testAtomVolumeString),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		tickerMap := map[string]MexcTicker{}
		tickerMap["ATOMUSDT"] = MexcTicker{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeString,
		}

		tickerMap["BTCUSDT"] = MexcTicker{
			Price:  testBtcPriceFloat64,
			Volume: testBtcVolumeString,
		}

		p.tickers = tickerMap
		prices, err := p.GetTickerPrices(
			testAtomUsdtCurrencyPair,
			testBtcUsdtCurrencyPair,
		)
		require.NoError(t, err)
		require.Len(t, prices, 2)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testBtcPriceFloat64)),
			prices["BTCUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(testBtcVolumeString),
			prices["BTCUSDT"].Volume,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testAtomPriceFloat64)),
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
		require.Error(t, err)
		require.Equal(t, "mexc failed to get ticker price for FOOBAR", err.Error())
		require.Nil(t, prices)
	})
}

func TestMexcProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &MexcProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		testAtomUsdtCurrencyPair,
		testBtcUsdtCurrencyPair,
	}
	subMsgs := provider.GetSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(
		t,
		`{"method":"SUBSCRIPTION","params":["spot@public.kline.v3.api@ATOMUSDT@Min15","spot@public.kline.v3.api@BTCUSDT@Min15"]}`,
		string(msg),
	)
}
