package oracle

import (
	"context"
	"fmt"
	"testing"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"price-feeder/config"
	"price-feeder/oracle/client"
	"price-feeder/oracle/derivative"
	"price-feeder/oracle/history"
	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"
)

type mockProvider struct {
	prices map[string]types.TickerPrice
}

func (m mockProvider) GetTickerPrices(_ ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return m.prices, nil
}

func (m mockProvider) SubscribeCurrencyPairs(_ ...types.CurrencyPair) error {
	return nil
}

func (m mockProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}

func (m mockProvider) SetPairs([]types.CurrencyPair) error {
	return nil
}

func (m mockProvider) CurrencyPairToProviderPair(pair types.CurrencyPair) string {
	return ""
}

// func (m mockProvider) ProviderPairToCurrencyPair(pair string) types.CurrencyPair {
// 	return types.CurrencyPair{}
// }

type failingProvider struct {
	mockProvider
}

func (m failingProvider) GetTickerPrices(_ ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return nil, fmt.Errorf("unable to get ticker prices")
}

type OracleTestSuite struct {
	suite.Suite

	oracle *Oracle
}

// SetupSuite executes once before the suite's tests are executed.
func (ots *OracleTestSuite) SetupSuite() {
	history, err := history.NewPriceHistory(":memory:", zerolog.Nop())
	ots.NoError(err)
	ots.oracle = New(
		zerolog.Nop(),
		client.OracleClient{},
		[]config.CurrencyPair{
			{
				Base:      "UMEE",
				Quote:     "USDT",
				Providers: []provider.Name{provider.ProviderBinance},
			},
			{
				Base:      "UMEE",
				Quote:     "USDC",
				Providers: []provider.Name{provider.ProviderKraken},
			},
			{
				Base:      "XBT",
				Quote:     "USDT",
				Providers: []provider.Name{provider.ProviderOsmosis},
			},
			{
				Base:      "USDC",
				Quote:     "USD",
				Providers: []provider.Name{provider.ProviderHuobi},
			},
			{
				Base:      "USDT",
				Quote:     "USD",
				Providers: []provider.Name{provider.ProviderCoinbase},
			},
		},
		time.Millisecond*100,
		make(map[string]sdk.Dec),
		make(map[string]int),
		make(map[provider.Name]provider.Endpoint),
		map[string]derivative.Derivative{},
		map[string][]types.CurrencyPair{},
		map[string]struct{}{},
		[]config.Healthchecks{
			{URL: "https://hc-ping.com/HEALTHCHECK-UUID", Timeout: "200ms"},
		},
		history,
		nil,
		nil,
	)
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(OracleTestSuite))
}

func (ots *OracleTestSuite) TestStop() {
	ots.Eventually(
		func() bool {
			ots.oracle.Stop()
			return true
		},
		5*time.Second,
		time.Second,
	)
}

func (ots *OracleTestSuite) TestGetLastPriceSyncTimestamp() {
	// when no tick() has been invoked, assume zero value
	ots.Require().Equal(time.Time{}, ots.oracle.GetLastPriceSyncTimestamp())
}

func (ots *OracleTestSuite) TestPrices() {
	// initial prices should be empty (not set)
	ots.Require().Empty(ots.oracle.GetPrices())

	// use a mock provider without a conversion rate for these stablecoins
	ots.oracle.priceProviders = map[provider.Name]provider.Provider{
		provider.ProviderBinance: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDT": {
					Price:  sdk.MustNewDecFromStr("3.72"),
					Volume: sdk.MustNewDecFromStr("2396974.02000000"),
				},
			},
		},
		provider.ProviderKraken: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDC": {
					Price:  sdk.MustNewDecFromStr("3.70"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
	}

	ots.Require().NoError(ots.oracle.SetPrices(context.TODO()))

	prices := ots.oracle.GetPrices()
	ots.Require().Len(prices, 0)

	// use a mock provider to provide prices for the configured exchange pairs
	ots.oracle.priceProviders = map[provider.Name]provider.Provider{
		provider.ProviderBinance: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDT": {
					Price:  sdk.MustNewDecFromStr("3.72"),
					Volume: sdk.MustNewDecFromStr("2396974.02000000"),
				},
			},
		},
		provider.ProviderKraken: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDC": {
					Price:  sdk.MustNewDecFromStr("3.70"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
		provider.ProviderHuobi: mockProvider{
			prices: map[string]types.TickerPrice{
				"USDCUSD": {
					Price:  sdk.MustNewDecFromStr("1"),
					Volume: sdk.MustNewDecFromStr("2396974.34000000"),
				},
			},
		},
		provider.ProviderCoinbase: mockProvider{
			prices: map[string]types.TickerPrice{
				"USDTUSD": {
					Price:  sdk.MustNewDecFromStr("1"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
		provider.ProviderOsmosis: mockProvider{
			prices: map[string]types.TickerPrice{
				"XBTUSDT": {
					Price:  sdk.MustNewDecFromStr("3.717"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
	}

	ots.oracle.providerMinOverrides = map[string]int{
		"XBT":  1,
		"UMEE": 1,
		"USDT": 1,
		"USDC": 1,
	}

	ots.Require().NoError(ots.oracle.SetPrices(context.TODO()))

	prices = ots.oracle.GetPrices()
	ots.Require().Len(prices, 4)
	ots.Require().Equal(sdk.MustNewDecFromStr("3.710916056220858266"), prices.AmountOf("UMEE"))
	ots.Require().Equal(sdk.MustNewDecFromStr("3.717"), prices.AmountOf("XBT"))
	ots.Require().Equal(sdk.MustNewDecFromStr("1"), prices.AmountOf("USDC"))
	ots.Require().Equal(sdk.MustNewDecFromStr("1"), prices.AmountOf("USDT"))

	// use one working provider and one provider with an incorrect exchange rate
	ots.oracle.priceProviders = map[provider.Name]provider.Provider{
		provider.ProviderBinance: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDX": {
					Price:  sdk.MustNewDecFromStr("3.72"),
					Volume: sdk.MustNewDecFromStr("2396974.02000000"),
				},
			},
		},
		provider.ProviderKraken: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDC": {
					Price:  sdk.MustNewDecFromStr("3.70"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
		provider.ProviderHuobi: mockProvider{
			prices: map[string]types.TickerPrice{
				"USDCUSD": {
					Price:  sdk.MustNewDecFromStr("1"),
					Volume: sdk.MustNewDecFromStr("2396974.34000000"),
				},
			},
		},
		provider.ProviderCoinbase: mockProvider{
			prices: map[string]types.TickerPrice{
				"USDTUSD": {
					Price:  sdk.MustNewDecFromStr("1"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
		provider.ProviderOsmosis: mockProvider{
			prices: map[string]types.TickerPrice{
				"XBTUSDT": {
					Price:  sdk.MustNewDecFromStr("3.717"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
	}

	ots.Require().NoError(ots.oracle.SetPrices(context.TODO()))
	prices = ots.oracle.GetPrices()
	ots.Require().Len(prices, 4)
	ots.Require().Equal(sdk.MustNewDecFromStr("3.70"), prices.AmountOf("UMEE"))
	ots.Require().Equal(sdk.MustNewDecFromStr("3.717"), prices.AmountOf("XBT"))
	ots.Require().Equal(sdk.MustNewDecFromStr("1"), prices.AmountOf("USDC"))
	ots.Require().Equal(sdk.MustNewDecFromStr("1"), prices.AmountOf("USDT"))

	// use one working provider and one provider that fails
	ots.oracle.priceProviders = map[provider.Name]provider.Provider{
		provider.ProviderBinance: failingProvider{
			mockProvider: mockProvider{
				prices: map[string]types.TickerPrice{
					"UMEEUSDC": {
						Price:  sdk.MustNewDecFromStr("3.72"),
						Volume: sdk.MustNewDecFromStr("2396974.02000000"),
					},
				},
			},
		},
		provider.ProviderKraken: mockProvider{
			prices: map[string]types.TickerPrice{
				"UMEEUSDC": {
					Price:  sdk.MustNewDecFromStr("3.71"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
		provider.ProviderHuobi: mockProvider{
			prices: map[string]types.TickerPrice{
				"USDCUSD": {
					Price:  sdk.MustNewDecFromStr("1"),
					Volume: sdk.MustNewDecFromStr("2396974.34000000"),
				},
			},
		},
		provider.ProviderCoinbase: mockProvider{
			prices: map[string]types.TickerPrice{
				"USDTUSD": {
					Price:  sdk.MustNewDecFromStr("1"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
		provider.ProviderOsmosis: mockProvider{
			prices: map[string]types.TickerPrice{
				"XBTUSDT": {
					Price:  sdk.MustNewDecFromStr("3.717"),
					Volume: sdk.MustNewDecFromStr("1994674.34000000"),
				},
			},
		},
	}

	ots.Require().NoError(ots.oracle.SetPrices(context.TODO()))
	prices = ots.oracle.GetPrices()
	ots.Require().Len(prices, 4)
	ots.Require().Equal(sdk.MustNewDecFromStr("3.71"), prices.AmountOf("UMEE"))
	ots.Require().Equal(sdk.MustNewDecFromStr("3.717"), prices.AmountOf("XBT"))
	ots.Require().Equal(sdk.MustNewDecFromStr("1"), prices.AmountOf("USDC"))
	ots.Require().Equal(sdk.MustNewDecFromStr("1"), prices.AmountOf("USDT"))
}

func TestGenerateSalt(t *testing.T) {
	salt, err := GenerateSalt(0)
	require.Error(t, err)
	require.Empty(t, salt)

	salt, err = GenerateSalt(32)
	require.NoError(t, err)
	require.NotEmpty(t, salt)
}

func TestGenerateExchangeRatesString(t *testing.T) {
	testCases := map[string]struct {
		input    sdk.DecCoins
		expected string
	}{
		"empty input": {
			input:    sdk.NewDecCoins(),
			expected: "",
		},
		"single denom": {
			input:    sdk.NewDecCoins(sdk.NewDecCoinFromDec("UMEE", sdk.MustNewDecFromStr("3.72"))),
			expected: "3.720000000000000000UMEE",
		},
		"multi denom": {
			input: sdk.NewDecCoins(sdk.NewDecCoinFromDec("UMEE", sdk.MustNewDecFromStr("3.72")),
				sdk.NewDecCoinFromDec("ATOM", sdk.MustNewDecFromStr("40.13")),
				sdk.NewDecCoinFromDec("OSMO", sdk.MustNewDecFromStr("8.69")),
			),
			expected: "40.130000000000000000ATOM,8.690000000000000000OSMO,3.720000000000000000UMEE",
		},
	}

	for name, tc := range testCases {
		tc := tc

		t.Run(name, func(t *testing.T) {
			out := GenerateExchangeRatesString(tc.input)
			require.Equal(t, tc.expected, out)
		})
	}
}

func TestSuccessGetComputedPricesTickers(t *testing.T) {
	providerPrices := make(provider.AggregatedProviderPrices, 1)
	pair := types.CurrencyPair{
		Base:  "ATOM",
		Quote: "USD",
	}

	atomPrice := sdk.MustNewDecFromStr("29.93")
	atomVolume := sdk.MustNewDecFromStr("894123.00")

	tickerPrices := map[string]types.TickerPrice{}
	tickerPrices[pair.String()] = types.TickerPrice{
		Price:  atomPrice,
		Volume: atomVolume,
	}
	providerPrices[provider.ProviderBinance] = tickerPrices

	providerPair := map[provider.Name][]types.CurrencyPair{
		provider.ProviderBinance: {pair},
	}

	providerMinOverrides := map[string]int{
		"ATOM": 1,
	}

	prices, err := GetComputedPrices(
		zerolog.Nop(),
		providerPrices,
		providerPair,
		make(map[string]sdk.Dec),
		providerMinOverrides,
		nil,
	)

	require.NoError(t, err, "It should successfully get computed ticker prices")
	require.Equal(t, atomPrice, prices[pair.Base])
}

func TestGetComputedPricesTickersConversion(t *testing.T) {
	btcEthPair := types.CurrencyPair{
		Base:  "BTC",
		Quote: "ETH",
	}
	btcUsdPair := types.CurrencyPair{
		Base:  "BTC",
		Quote: "USD",
	}
	ethUsdPair := types.CurrencyPair{
		Base:  "ETH",
		Quote: "USD",
	}
	volume := sdk.MustNewDecFromStr("881272.00")
	btcEthPrice := sdk.MustNewDecFromStr("72.55")
	ethUsdPrice := sdk.MustNewDecFromStr("9989.02")
	btcUsdPrice := sdk.MustNewDecFromStr("724603.401")
	providerPrices := make(provider.AggregatedProviderPrices, 1)

	// normal rates
	binanceTickerPrices := make(map[string]types.TickerPrice, 2)
	binanceTickerPrices[btcEthPair.String()] = types.TickerPrice{
		Price:  btcEthPrice,
		Volume: volume,
	}
	binanceTickerPrices[ethUsdPair.String()] = types.TickerPrice{
		Price:  ethUsdPrice,
		Volume: volume,
	}
	providerPrices[provider.ProviderBinance] = binanceTickerPrices

	// normal rates
	gateTickerPrices := make(map[string]types.TickerPrice, 4)
	// gateTickerPrices[btcEthPair.String()] = types.TickerPrice{
	// 	Price:  btcEthPrice,
	// 	Volume: volume,
	// }
	gateTickerPrices[ethUsdPair.String()] = types.TickerPrice{
		Price:  ethUsdPrice,
		Volume: volume,
	}
	providerPrices[provider.ProviderGate] = gateTickerPrices

	// abnormal eth rate
	okxTickerPrices := make(map[string]types.TickerPrice, 1)
	okxTickerPrices[ethUsdPair.String()] = types.TickerPrice{
		Price:  sdk.MustNewDecFromStr("1.0"),
		Volume: volume,
	}
	providerPrices[provider.ProviderOkx] = okxTickerPrices

	// btc / usd rate
	krakenTickerPrices := make(map[string]types.TickerPrice, 1)
	krakenTickerPrices[btcUsdPair.String()] = types.TickerPrice{
		Price:  btcUsdPrice,
		Volume: volume,
	}
	providerPrices[provider.ProviderKraken] = krakenTickerPrices

	providerPair := map[provider.Name][]types.CurrencyPair{
		provider.ProviderBinance: {ethUsdPair, btcEthPair},
		provider.ProviderGate:    {ethUsdPair},
		provider.ProviderOkx:     {ethUsdPair},
		provider.ProviderKraken:  {btcUsdPair},
	}

	providerMinOverrides := map[string]int{
		"BTC": 1,
	}

	prices, err := GetComputedPrices(
		zerolog.Nop(),
		providerPrices,
		providerPair,
		make(map[string]sdk.Dec),
		providerMinOverrides,
		nil,
	)

	require.NoError(t, err,
		"It should successfully filter out bad tickers and convert everything to USD",
	)
	require.Equal(t,

		ethUsdPrice.Mul(
			btcEthPrice).Add(btcUsdPrice).Quo(sdk.MustNewDecFromStr("2")),
		prices[btcEthPair.Base],
	)
}
