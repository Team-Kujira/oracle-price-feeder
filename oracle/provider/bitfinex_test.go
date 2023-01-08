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

func TestBitfinexProvider_GetTickerPrices(t *testing.T) {
	p, err := NewBitfinexProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		channel := uint64(1)

		tickers := map[uint64]BitfinexTicker{}
		tickers[channel] = BitfinexTicker{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeFloat64,
		}

		p.tickers = tickers
		p.channels["ATOMUSDT"] = channel

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
		atomChannel := uint64(1)
		btcChannel := uint64(2)

		tickers := map[uint64]BitfinexTicker{}
		tickers[atomChannel] = BitfinexTicker{
			Price:  testAtomPriceFloat64,
			Volume: testAtomVolumeFloat64,
		}
		tickers[btcChannel] = BitfinexTicker{
			Price:  testBtcPriceFloat64,
			Volume: testBtcVolumeFloat64,
		}

		p.tickers = tickers
		p.channels["ATOMUSDT"] = atomChannel
		p.channels["BTCUSDT"] = btcChannel

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
		require.EqualError(t, err, "bitfinex failed to get channel id for FOOBAR")
		require.Nil(t, prices)
	})
}

func TestBitfinexProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &BitfinexProvider{
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
		`{"event":"subscribe","channel":"ticker","symbol":"BTCUSDT"}`,
		string(msg),
	)

	msg, _ = json.Marshal(msgs[1])
	require.Equal(
		t,
		`{"event":"subscribe","channel":"ticker","symbol":"ATOMUSDT"}`,
		string(msg),
	)
}
