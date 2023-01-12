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

func TestPhemexProvider_GetTickerPrices(t *testing.T) {
	p, err := NewPhemexProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]PhemexTickerMsg{}
		tickers["sATOMUSDT"] = PhemexTickerMsg{
			Data: PhemexTicker{
				Price:  testAtomPriceInt64,
				Volume: testAtomVolumeInt64,
			},
		}

		p.tickers = tickers

		prices, err := p.GetTickerPrices(testAtomUsdtCurrencyPair)

		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%d", testAtomPriceInt64)).QuoInt64(100000000),
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%d", testAtomVolumeInt64)).QuoInt64(100000000),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		tickers := map[string]PhemexTickerMsg{}
		tickers["sATOMUSDT"] = PhemexTickerMsg{
			Data: PhemexTicker{
				Price:  testAtomPriceInt64,
				Volume: testAtomVolumeInt64,
			},
		}
		tickers["sBTCUSDT"] = PhemexTickerMsg{
			Data: PhemexTicker{
				Price:  testBtcPriceInt64,
				Volume: testBtcVolumeInt64,
			},
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
			sdk.MustNewDecFromStr(fmt.Sprintf("%d", testBtcPriceInt64)).QuoInt64(100000000),
			prices["BTCUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%d", testBtcVolumeInt64)).QuoInt64(100000000),
			prices["BTCUSDT"].Volume,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%d", testAtomPriceInt64)).QuoInt64(100000000),
			prices["ATOMUSDT"].Price,
		)
		require.Equal(
			t,
			sdk.MustNewDecFromStr(fmt.Sprintf("%d", testAtomVolumeInt64)).QuoInt64(100000000),
			prices["ATOMUSDT"].Volume,
		)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.EqualError(t, err, "phemex failed to get ticker price for sFOOBAR")
		require.Nil(t, prices)
	})
}

func TestPhemexProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &PhemexProvider{
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
		`{"id":1,"method":"spot_market24h.subscribe","params":[]}`,
		string(msg),
	)
}
