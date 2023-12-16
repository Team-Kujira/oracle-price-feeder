package oracle

import (
	"testing"

	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestConvertTickersToUsdChaining(t *testing.T) {
	providerPrices := provider.AggregatedProviderPrices{}

	osmosisTickers := map[string]types.TickerPrice{
		"STATOMATOM": {
			Price:  sdk.MustNewDecFromStr("1.1"),
			Volume: sdk.MustNewDecFromStr("1"),
		},
		"STOSMOOSMO": {
			Price:  sdk.MustNewDecFromStr("1.1"),
			Volume: sdk.MustNewDecFromStr("1"),
		},
	}
	providerPrices[provider.ProviderOsmosis] = osmosisTickers

	binanceTickers := map[string]types.TickerPrice{
		"ATOMUSDT": {
			Price:  sdk.MustNewDecFromStr("10"),
			Volume: sdk.MustNewDecFromStr("1"),
		},
	}
	providerPrices[provider.ProviderBinance] = binanceTickers

	coinbaseTickers := map[string]types.TickerPrice{
		"USDTUSD": {
			Price:  sdk.MustNewDecFromStr("0.999"),
			Volume: sdk.MustNewDecFromStr("1"),
		},
		"OSMOUSD": {
			Price:  sdk.MustNewDecFromStr("0.8"),
			Volume: sdk.MustNewDecFromStr("1"),
		},
	}
	providerPrices[provider.ProviderKraken] = coinbaseTickers

	providerPairs := map[provider.Name][]types.CurrencyPair{
		provider.ProviderOsmosis: {types.CurrencyPair{
			Base:  "STATOM",
			Quote: "ATOM",
		}, types.CurrencyPair{
			Base:  "STOSMO",
			Quote: "OSMO",
		}},
		provider.ProviderBinance: {types.CurrencyPair{
			Base:  "ATOM",
			Quote: "USDT",
		}},
		provider.ProviderCoinbase: {types.CurrencyPair{
			Base:  "USDT",
			Quote: "USD",
		}, types.CurrencyPair{
			Base:  "OSMO",
			Quote: "USD",
		}},
	}

	providerMinOverrides := map[string]int{
		"STATOM": 1,
		"STOSMO": 1,
		"USDT":   1,
		"OSMO":   1,
		"ATOM":   1,
	}

	convertedTickers, err := convertTickersToUSD(
		zerolog.Nop(),
		providerPrices,
		providerPairs,
		make(map[string]sdk.Dec),
		providerMinOverrides,
		nil,
	)
	require.NoError(t, err)

	require.Equal(
		t,
		convertedTickers["STATOM"],
		sdk.MustNewDecFromStr("10.989"),
	)

	require.Equal(
		t,
		convertedTickers["STOSMO"],
		sdk.MustNewDecFromStr("0.88"),
	)
}

func TestConvertTickersToUSDFiltering(t *testing.T) {
	providerPrices := provider.AggregatedProviderPrices{}

	krakenTickers := map[string]types.TickerPrice{
		"BTCUSDT": {
			Price:  sdk.MustNewDecFromStr("30000"),
			Volume: sdk.MustNewDecFromStr("10"),
		},
	}
	providerPrices[provider.ProviderKraken] = krakenTickers

	binanceTickers := map[string]types.TickerPrice{
		"BTCUSDT": {
			Price:  sdk.MustNewDecFromStr("30010"),
			Volume: sdk.MustNewDecFromStr("10"),
		},
	}
	providerPrices[provider.ProviderBinance] = binanceTickers

	kucoinTickers := map[string]types.TickerPrice{
		"BTCUSDT": {
			Price:  sdk.MustNewDecFromStr("30020"),
			Volume: sdk.MustNewDecFromStr("100"),
		},
	}
	providerPrices[provider.ProviderKucoin] = kucoinTickers

	coinbaseTickers := map[string]types.TickerPrice{
		"BTCUSDT": {
			Price:  sdk.MustNewDecFromStr("30450"),
			Volume: sdk.MustNewDecFromStr("10000"),
		},
		"USDTUSD": {
			Price:  sdk.MustNewDecFromStr("1"),
			Volume: sdk.MustNewDecFromStr("10000"),
		},
	}
	providerPrices[provider.ProviderCoinbase] = coinbaseTickers

	btcUsdt := types.CurrencyPair{
		Base:  "BTC",
		Quote: "USDT",
	}

	usdtUsd := types.CurrencyPair{
		Base:  "USDT",
		Quote: "USD",
	}

	providerPairs := map[provider.Name][]types.CurrencyPair{
		provider.ProviderKraken:   {btcUsdt},
		provider.ProviderBinance:  {btcUsdt},
		provider.ProviderKucoin:   {btcUsdt},
		provider.ProviderCoinbase: {btcUsdt, usdtUsd},
	}

	prividerMinOverrides := map[string]int{
		"USDT": 1,
		"BTC":  1,
	}

	rates, err := convertTickersToUSD(
		zerolog.Nop(),
		providerPrices,
		providerPairs,
		make(map[string]sdk.Dec),
		prividerMinOverrides,
		nil,
	)
	require.NoError(t, err)

	// skip BTC/USDT from Coinbase
	// (30000*10+30010*10+30020*100) / 120 = 30017.5

	require.Equal(
		t,
		sdk.MustNewDecFromStr("30017.5"),
		rates["BTC"],
	)
}

func TestConvertTickersToUsdVwap(t *testing.T) {
	providerPrices := provider.AggregatedProviderPrices{}

	binanceTickers := map[string]types.TickerPrice{
		"ETHBTC": {
			Price:  sdk.MustNewDecFromStr("0.066"),
			Volume: sdk.MustNewDecFromStr("100"),
		},
		"BTCUSDT": {
			Price:  sdk.MustNewDecFromStr("30000"),
			Volume: sdk.MustNewDecFromStr("55"),
		},
	}
	providerPrices[provider.ProviderBinance] = binanceTickers

	coinbaseTickers := map[string]types.TickerPrice{
		"BTCUSD": {
			Price:  sdk.MustNewDecFromStr("30050"),
			Volume: sdk.MustNewDecFromStr("45"),
		},
		"USDTUSD": {
			Price:  sdk.MustNewDecFromStr("0.999"),
			Volume: sdk.MustNewDecFromStr("100000"),
		},
	}
	providerPrices[provider.ProviderCoinbase] = coinbaseTickers

	providerPairs := map[provider.Name][]types.CurrencyPair{
		provider.ProviderBinance: {
			types.CurrencyPair{Base: "ETH", Quote: "BTC"},
			types.CurrencyPair{Base: "BTC", Quote: "USDT"},
		},
		provider.ProviderCoinbase: {
			types.CurrencyPair{Base: "BTC", Quote: "USD"},
			types.CurrencyPair{Base: "USDT", Quote: "USD"},
		},
	}

	providerMinOverrides := map[string]int{
		"BTC":  1,
		"ETH":  1,
		"USDT": 1,
	}

	rates, err := convertTickersToUSD(
		zerolog.Nop(),
		providerPrices,
		providerPairs,
		make(map[string]sdk.Dec),
		providerMinOverrides,
		nil,
	)
	require.NoError(t, err)

	// VWAP( BTCUSDT * USDTUSD, BTCUSD )
	// ((30000*0.999*55*+30050*45) / 100 = 30006.0

	require.Equal(
		t,
		sdk.MustNewDecFromStr("30006.0"),
		rates["BTC"],
	)

	// BTCUSD * ETHBTC
	// 30050 * 0.066 = 1983.3

	require.Equal(
		t,
		sdk.MustNewDecFromStr("1983.3"),
		rates["ETH"],
	)
}

func TestConvertTickersToUsdEmptyProvider(t *testing.T) {
	providerPrices := provider.AggregatedProviderPrices{}

	providerPrices[provider.ProviderBinance] = map[string]types.TickerPrice{}

	providerPairs := map[provider.Name][]types.CurrencyPair{
		provider.ProviderBinance: {
			types.CurrencyPair{Base: "BTC", Quote: "USD"},
		},
	}

	rates, err := convertTickersToUSD(
		zerolog.Nop(),
		providerPrices,
		providerPairs,
		make(map[string]sdk.Dec),
		make(map[string]int),
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, 0, len(rates))
}

func TestConvertTickersToUsdEmptyPrices(t *testing.T) {
	providerPrices := provider.AggregatedProviderPrices{}

	providerPairs := map[provider.Name][]types.CurrencyPair{}

	rates, err := convertTickersToUSD(
		zerolog.Nop(),
		providerPrices,
		providerPairs,
		make(map[string]sdk.Dec),
		make(map[string]int),
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, 0, len(rates))
}
