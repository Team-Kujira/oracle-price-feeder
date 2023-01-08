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

func TestBitforexProvider_GetTickerPrices(t *testing.T) {
	p, err := NewBitforexProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		tickers := map[string]BitforexTickerData{}
		tickers["coin-usdt-atom"] = BitforexTickerData{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeFloat64,
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
		tickers := map[string]BitforexTickerData{}
		tickers["coin-usdt-atom"] = BitforexTickerData{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeFloat64,
		}
		tickers["coin-usdt-btc"] = BitforexTickerData{
			Price:  testBtcPriceFloat64,
			Volume: testBtcVolumeFloat64,
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
		require.EqualError(t, err, "bitforex failed to get ticker price for FOOBAR")
		require.Nil(t, prices)
	})
}

func TestBitforexProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &BitforexProvider{
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
		`[{"type":"subHq","event":"ticker","param":{"businessType":"coin-usdt-btc"}},`+
			`{"type":"subHq","event":"ticker","param":{"businessType":"coin-usdt-atom"}}]`,
		string(msg),
	)
}
