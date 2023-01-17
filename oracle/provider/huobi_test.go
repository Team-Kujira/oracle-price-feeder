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

func TestHuobiProvider_GetTickerPrices(t *testing.T) {
	p, err := NewHuobiProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		types.CurrencyPair{Base: "ATOM", Quote: "USDT"},
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickerMap := map[string]HuobiTicker{}
		tickerMap["market.atomusdt.trade.detail"] = HuobiTicker{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeFloat64,
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
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testAtomVolumeFloat64)),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		tickerMap := map[string]HuobiTicker{}
		tickerMap["market.atomusdt.trade.detail"] = HuobiTicker{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeFloat64,
		}

		tickerMap["market.btcusdt.trade.detail"] = HuobiTicker{
			Price:  testBtcPriceFloat64,
			Volume: testBtcVolumeFloat64,
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
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testBtcVolumeFloat64)),
			prices["BTCUSDT"].Volume,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testAtomPriceFloat64)),
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%f", testAtomVolumeFloat64)),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.EqualError(t, err, "huobi failed to get ticker price for FOOBAR")
		require.Nil(t, prices)
	})
}

func TestHuobiProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &HuobiProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		testAtomUsdtCurrencyPair,
		testBtcUsdtCurrencyPair,
	}
	subMsgs := provider.GetSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(t, `{"sub":"market.atomusdt.trade.detail"}`, string(msg))

	msg, _ = json.Marshal(subMsgs[1])
	require.Equal(t, `{"sub":"market.btcusdt.trade.detail"}`, string(msg))
}
