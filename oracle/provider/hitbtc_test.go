package provider

import (
	"context"
	"encoding/json"
	"price-feeder/oracle/types"
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestHitbtcProvider_GetTickerPrices(t *testing.T) {
	p, err := NewHitbtcProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]HitbtcTicker{}
		tickers["ATOMUSDT"] = HitbtcTicker{
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
		tickers := map[string]HitbtcTicker{}
		tickers["ATOMUSDT"] = HitbtcTicker{
			Price:  testAtomPriceString,
			Volume: testAtomVolumeString,
		}
		tickers["BTCUSDT"] = HitbtcTicker{
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
		require.EqualError(t, err, "hitbtc failed to get ticker price for FOOBAR")
		require.Nil(t, prices)
	})
}

func TestHitbtcProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &HitbtcProvider{
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
		`{"ch":"ticker/price/1s","method":"subscribe","params":{"symbols":["BTCUSDT","ATOMUSDT"]}}`,
		string(msg),
	)
}
