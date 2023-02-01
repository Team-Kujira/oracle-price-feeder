package provider

import (
	"context"
	"encoding/json"
	"price-feeder/oracle/types"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestBybitProvider_GetTickerPrices(t *testing.T) {
	p, err := NewBybitProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		testAtomUsdtCurrencyPair,
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		p.tickers = testTickersAtom
		prices, err := p.GetTickerPrices(testAtomUsdtCurrencyPair)
		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(t, testAtomPriceDec, prices["ATOMUSDT"].Price)
		require.Equal(t, testAtomVolumeDec, prices["ATOMUSDT"].Volume)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		p.tickers = testTickersAtomBtc
		prices, err := p.GetTickerPrices(
			testAtomUsdtCurrencyPair,
			testBtcUsdtCurrencyPair,
		)
		require.NoError(t, err)
		require.Len(t, prices, 2)
		require.Equal(t, testAtomPriceDec, prices["ATOMUSDT"].Price)
		require.Equal(t, testAtomVolumeDec, prices["ATOMUSDT"].Volume)
		require.Equal(t, testBtcPriceDec, prices["BTCUSDT"].Price)
		require.Equal(t, testBtcVolumeDec, prices["BTCUSDT"].Volume)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, _ := p.GetTickerPrices(testFooBarCurrencyPair)
		require.Equal(t, map[string]types.TickerPrice{}, prices)
	})
}

func TestBybitProvider_GetSubscriptionMsgs(t *testing.T) {
	provider := &BybitProvider{
		provider: provider{
			pairs: map[string]types.CurrencyPair{},
		},
	}
	cps := []types.CurrencyPair{
		testAtomUsdtCurrencyPair,
		testBtcUsdtCurrencyPair,
	}
	subMsgs := provider.getSubscriptionMsgs(cps...)
	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(t, `{"op":"subscribe","args":["tickers.ATOMUSDT","tickers.BTCUSDT"]}`, string(msg))
}
