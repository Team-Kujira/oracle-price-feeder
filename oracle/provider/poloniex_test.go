package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"price-feeder/oracle/types"
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestPoloniexProvider_GetTickerPrices(t *testing.T) {
	p, err := NewPoloniexProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]PoloniexTicker{}
		tickers["ATOM_USDT"] = PoloniexTicker{
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
		tickers := map[string]PoloniexTicker{}
		tickers["ATOM_USDT"] = PoloniexTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
		}
		tickers["BTC_USDT"] = PoloniexTicker{
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
		require.EqualError(t, err, "poloniex failed to get ticker price for FOO_BAR")
		require.Nil(t, prices)
	})
}

func TestPoloniexProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &PoloniexProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		testBtcUsdtCurrencyPair,
		testAtomUsdtCurrencyPair,
	}

	msgs := provider.GetSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(msgs[0])
	require.Equal(
		t,
		`{"event":"subscribe","channel":["ticker"],"symbols":["BTC_USDT","ATOM_USDT"]}`,
		string(msg),
	)
}
