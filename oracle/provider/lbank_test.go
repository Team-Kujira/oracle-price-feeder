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

func TestLbankProvider_GetTickerPrices(t *testing.T) {
	p, err := NewLbankProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]LbankTickerMsg{}
		tickers["atom_usdt"] = LbankTickerMsg{
			Tick: LbankTicker{
				Price:  testAtomPriceFloat64,
				Volume: testAtomVolumeFloat64,
			},
			Time: "2023-01-12T02:52:42.469",
		}

		p.tickers = tickers

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
		tickers := map[string]LbankTickerMsg{}
		tickers["atom_usdt"] = LbankTickerMsg{
			Tick: LbankTicker{
				Price:  testAtomPriceFloat64,
				Volume: testAtomVolumeFloat64,
			},
			Time: "2023-01-12T02:52:42.469",
		}
		tickers["btc_usdt"] = LbankTickerMsg{
			Tick: LbankTicker{
				Price:  testBtcPriceFloat64,
				Volume: testBtcVolumeFloat64,
			},
			Time: "2023-01-12T02:52:42.469",
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
		require.EqualError(t, err, "lbank failed to get ticker price for foo_bar")
		require.Nil(t, prices)
	})
}

func TestLbankProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &LbankProvider{
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
		`{"action":"subscribe","subscribe":"tick","pair":"btc_usdt"}`,
		string(msg),
	)

	msg, _ = json.Marshal(msgs[1])
	require.Equal(
		t,
		`{"action":"subscribe","subscribe":"tick","pair":"atom_usdt"}`,
		string(msg),
	)
}
