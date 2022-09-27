package provider

import (
	"context"
	"testing"

	"price-feeder/config"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestLiquidProvider_GetTickerPrices(t *testing.T) {
	p, err := NewLiquidProvider(
		context.TODO(),
		zerolog.Nop(),
		config.ProviderEndpoint{},
		types.CurrencyPair{Base: "stATOM", Quote: "ATOM"},
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		lastPrice := "34.69000000"
		volume := "2396974.02000000"

		tickerMap := map[string]LiquidTicker{}
		tickerMap["STATOMATOM"] = LiquidTicker{
			Symbol:    "STATOMATOM",
			LastPrice: lastPrice,
			Volume:    volume,
		}

		p.tickers = tickerMap

		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "stATOM", Quote: "ATOM"})
		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(t, sdk.MustNewDecFromStr(lastPrice), prices["STATOMATOM"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["STATOMATOM"].Volume)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		lastPricestAtom := "34.69000000"
		volume := "2396974.02000000"

		tickerMap := map[string]LiquidTicker{}
		tickerMap["STATOMATOM"] = LiquidTicker{
			Symbol:    "STATOMATOM",
			LastPrice: lastPricestAtom,
			Volume:    volume,
		}

		p.tickers = tickerMap
		prices, err := p.GetTickerPrices(
			types.CurrencyPair{Base: "stATOM", Quote: "ATOM"},
		)
		require.NoError(t, err)
		require.Len(t, prices, 2)
		require.Equal(t, sdk.MustNewDecFromStr(lastPricestAtom), prices["STATOMATOM"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["STATOMATOM"].Volume)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.Error(t, err)
		require.Equal(t, "missing exchange rate for FOOBAR", err.Error())
		require.Nil(t, prices)
	})
}

func TestLiquidProvider_SubscribeCurrencyPairs(t *testing.T) {
	p, err := NewLiquidProvider(
		context.TODO(),
		zerolog.Nop(),
		config.ProviderEndpoint{},
		types.CurrencyPair{Base: "stATOM", Quote: "ATOM"},
	)
	require.NoError(t, err)

	t.Run("invalid_subscribe_channels_empty", func(t *testing.T) {
		err = p.SubscribeCurrencyPairs([]types.CurrencyPair{}...)
		require.ErrorContains(t, err, "currency pairs is empty")
	})
}

func TestLiquidCurrencyPairToLiquidPair(t *testing.T) {
	cp := types.CurrencyPair{Base: "stATOM", Quote: "ATOM"}
	liquidSymbol := currencyPairToLiquidPair(cp)
	require.Equal(t, liquidSymbol, "STATOM_ATOM")
}
