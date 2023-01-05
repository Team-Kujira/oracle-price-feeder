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

func TestBybitProvider_GetTickerPrices(t *testing.T) {
	p, err := NewBybitProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{},
		types.CurrencyPair{Base: "ATOM", Quote: "USDT"},
	)
	require.NoError(t, err)

	t.Run("valid_request_single_ticker", func(t *testing.T) {
		lastPrice := "34.69000000"
		volume := "2396974.02000000"

		tickerMap := map[string]BybitTicker{}
		tickerMap["tickers.ATOMUSDT"] = BybitTicker{
			Topic: "tickers.ATOMUSDT",
			Data: BybitTickerData{
				Price:  lastPrice,
				Volume: volume,
				Symbol: "ATOMUSDT",
				Time:   16729462890000,
			},
		}

		p.tickers = tickerMap

		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "ATOM", Quote: "USDT"})
		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(t, sdk.MustNewDecFromStr(lastPrice), prices["ATOMUSDT"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["ATOMUSDT"].Volume)
	})

	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		lastPriceAtom := "12.345"
		lastPriceLuna := "6.7890"
		volume := "12345678.9"

		tickerMap := map[string]BybitTicker{}
		tickerMap["tickers.ATOMUSDT"] = BybitTicker{
			Topic: "tickers.ATOMUSDT",
			Data: BybitTickerData{
				Price:  lastPriceAtom,
				Volume: volume,
				Symbol: "ATOMUSDT",
				Time:   16729462890000,
			},
		}

		tickerMap["tickers.LUNAUSDT"] = BybitTicker{
			Topic: "tickers.LUNAUSDT",
			Data: BybitTickerData{
				Price:  lastPriceLuna,
				Volume: volume,
				Symbol: "LUNAUSDT",
				Time:   16729462890000,
			},
		}

		p.tickers = tickerMap
		prices, err := p.GetTickerPrices(
			types.CurrencyPair{Base: "ATOM", Quote: "USDT"},
			types.CurrencyPair{Base: "LUNA", Quote: "USDT"},
		)
		require.NoError(t, err)
		require.Len(t, prices, 2)
		require.Equal(t, sdk.MustNewDecFromStr(lastPriceAtom), prices["ATOMUSDT"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["ATOMUSDT"].Volume)
		require.Equal(t, sdk.MustNewDecFromStr(lastPriceLuna), prices["LUNAUSDT"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["LUNAUSDT"].Volume)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.Error(t, err)
		require.Equal(t, "bybit failed to get ticker price for FOOBAR", err.Error())
		require.Nil(t, prices)
	})
}

func TestBybitProvider_getSubscriptionMsgs(t *testing.T) {
	provider := &BybitProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		{Base: "ATOM", Quote: "USDT"},
	}
	subMsgs := provider.getSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(t, "{\"op\":\"subscribe\",\"args\":[\"tickers.ATOMUSDT\"]}", string(msg))
}
