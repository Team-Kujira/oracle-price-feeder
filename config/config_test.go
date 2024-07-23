package config_test

import (
	"io/ioutil"
	"os"
	"testing"

	"price-feeder/config"
	"price-feeder/oracle/provider"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	validConfig := func() config.Config {
		return config.Config{
			Server: config.Server{
				ListenAddr:     "0.0.0.0:7171",
				VerboseCORS:    false,
				AllowedOrigins: []string{},
			},
			CurrencyPairs: []config.CurrencyPair{
				{Base: "ATOM", Quote: "USDT", Providers: []provider.Name{provider.ProviderKraken}},
			},
			Telemetry: config.Telemetry{
				ServiceName:             "price-feeder",
				Enabled:                 true,
				EnableHostname:          true,
				EnableHostnameLabel:     true,
				EnableServiceLabel:      true,
				GlobalLabels:            make([][]string, 1),
				PrometheusRetentionTime: 120,
			},
			Healthchecks: []config.Healthchecks{
				{URL: "https://hc-ping.com/HEALTHCHECK-UUID", Timeout: "200ms"},
			},
		}
	}
	emptyPairs := validConfig()
	emptyPairs.CurrencyPairs = []config.CurrencyPair{}

	invalidBase := validConfig()
	invalidBase.CurrencyPairs = []config.CurrencyPair{
		{Base: "", Quote: "USDT", Providers: []provider.Name{provider.ProviderKraken}},
	}

	invalidQuote := validConfig()
	invalidQuote.CurrencyPairs = []config.CurrencyPair{
		{Base: "ATOM", Quote: "", Providers: []provider.Name{provider.ProviderKraken}},
	}

	emptyProviders := validConfig()
	emptyProviders.CurrencyPairs = []config.CurrencyPair{
		{Base: "ATOM", Quote: "USDT", Providers: []provider.Name{}},
	}

	invalidEndpoints := validConfig()
	invalidEndpoints.ProviderEndpoints = []config.ProviderEndpoints{
		{
			Name: provider.ProviderBinance,
		},
	}

	invalidEndpointsProvider := validConfig()
	invalidEndpointsProvider.ProviderEndpoints = []config.ProviderEndpoints{
		{
			Name:      "foo",
			Urls:      []string{"bar"},
			Websocket: "baz",
		},
	}

	testCases := []struct {
		name      string
		cfg       config.Config
		expectErr bool
	}{
		{
			"valid config",
			validConfig(),
			false,
		},
		{
			"empty pairs",
			emptyPairs,
			true,
		},
		{
			"invalid base",
			invalidBase,
			true,
		},
		{
			"invalid quote",
			invalidQuote,
			true,
		},
		{
			"empty providers",
			emptyProviders,
			true,
		},
		{
			"invalid endpoints",
			invalidEndpoints,
			true,
		},
		{
			"invalid endpoint provider",
			invalidEndpointsProvider,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.cfg.Validate() != nil, tc.expectErr)
		})
	}
}

func TestParseConfig_Valid(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "price-feeder.toml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := []byte(`
[server]
listen_addr = "0.0.0.0:99999"
read_timeout = "20s"
verbose_cors = true
write_timeout = "20s"

[[currency_pairs]]
base = "ATOM"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "KUJI"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "USDT"
quote = "USD"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "STATOM"
quote = "ATOM"
providers = ["osmosisv2"]
derivative = "twap"
derivative_period = "30m"

[telemetry]
service_name = "price-feeder"
enabled = true
enable_hostname = true
enable_hostname_label = true
enable_service_label = true
prometheus_retention = 120
global_labels = [["chain-id", "kujira-local-testnet"]]

[[healthchecks]]
url = "https://hc-ping.com/HEALTHCHECK-UUID"
timeout = "200ms"
`)
	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	cfg, err := config.ParseConfig(tmpFile.Name())
	require.NoError(t, err)

	require.Equal(t, "0.0.0.0:99999", cfg.Server.ListenAddr)
	require.Equal(t, "20s", cfg.Server.WriteTimeout)
	require.Equal(t, "20s", cfg.Server.ReadTimeout)
	require.True(t, cfg.Server.VerboseCORS)
	require.Len(t, cfg.CurrencyPairs, 4)
	require.Equal(t, "ATOM", cfg.CurrencyPairs[0].Base)
	require.Equal(t, "USDT", cfg.CurrencyPairs[0].Quote)
	require.Len(t, cfg.CurrencyPairs[0].Providers, 3)
	require.Equal(t, provider.ProviderKraken, cfg.CurrencyPairs[0].Providers[0])
	require.Equal(t, provider.ProviderBinance, cfg.CurrencyPairs[0].Providers[1])
	require.Equal(t, "twap", cfg.CurrencyPairs[3].Derivative)
}

func TestParseConfig_Valid_NoTelemetry(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "price-feeder.toml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := []byte(`
[server]
listen_addr = "0.0.0.0:99999"
read_timeout = "20s"
verbose_cors = true
write_timeout = "20s"

[[currency_pairs]]
base = "ATOM"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "KUJI"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "USDT"
quote = "USD"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[telemetry]
enabled = false
`)
	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	cfg, err := config.ParseConfig(tmpFile.Name())
	require.NoError(t, err)

	require.Equal(t, "0.0.0.0:99999", cfg.Server.ListenAddr)
	require.Equal(t, "20s", cfg.Server.WriteTimeout)
	require.Equal(t, "20s", cfg.Server.ReadTimeout)
	require.True(t, cfg.Server.VerboseCORS)
	require.Len(t, cfg.CurrencyPairs, 3)
	require.Equal(t, "ATOM", cfg.CurrencyPairs[0].Base)
	require.Equal(t, "USDT", cfg.CurrencyPairs[0].Quote)
	require.Len(t, cfg.CurrencyPairs[0].Providers, 3)
	require.Equal(t, provider.ProviderKraken, cfg.CurrencyPairs[0].Providers[0])
	require.Equal(t, provider.ProviderBinance, cfg.CurrencyPairs[0].Providers[1])
	require.Equal(t, cfg.Telemetry.Enabled, false)
}

func TestParseConfig_InvalidProvider(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "price-feeder.toml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := []byte(`
listen_addr = ""

[[currency_pairs]]
base = "ATOM"
quote = "USD"
providers = [
	"kraken",
	"binance"
]

[[currency_pairs]]
base = "UMEE"
quote = "USD"
providers = [
	"kraken",
	"foobar"
]
`)
	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	_, err = config.ParseConfig(tmpFile.Name())
	require.Error(t, err)
}

func TestParseConfig_NonUSDQuote(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "price-feeder.toml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := []byte(`
listen_addr = ""

[[currency_pairs]]
base = "ATOM"
quote = "USDT"
providers = [
	"kraken",
	"binance"
]

[[currency_pairs]]
base = "UMEE"
quote = "USDT"
providers = [
	"kraken",
	"binance"
]
`)
	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	_, err = config.ParseConfig(tmpFile.Name())
	require.Error(t, err)
}

func TestParseConfig_Valid_Deviations(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "price-feeder.toml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := []byte(`
[server]
listen_addr = "0.0.0.0:99999"
read_timeout = "20s"
verbose_cors = true
write_timeout = "20s"

[[deviation_thresholds]]
base = "USDT"
threshold = "2"

[[deviation_thresholds]]
base = "ATOM"
threshold = "1.5"

[[currency_pairs]]
base = "ATOM"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "KUJI"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "USDT"
quote = "USD"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[telemetry]
service_name = "price-feeder"
enabled = true
enable_hostname = true
enable_hostname_label = true
enable_service_label = true
prometheus_retention = 120
global_labels = [["chain-id", "kujira-local-testnet"]]
`)
	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	cfg, err := config.ParseConfig(tmpFile.Name())
	require.NoError(t, err)

	require.Equal(t, "0.0.0.0:99999", cfg.Server.ListenAddr)
	require.Equal(t, "20s", cfg.Server.WriteTimeout)
	require.Equal(t, "20s", cfg.Server.ReadTimeout)
	require.True(t, cfg.Server.VerboseCORS)
	require.Len(t, cfg.CurrencyPairs, 3)
	require.Equal(t, "ATOM", cfg.CurrencyPairs[0].Base)
	require.Equal(t, "USDT", cfg.CurrencyPairs[0].Quote)
	require.Len(t, cfg.CurrencyPairs[0].Providers, 3)
	require.Equal(t, provider.ProviderKraken, cfg.CurrencyPairs[0].Providers[0])
	require.Equal(t, provider.ProviderBinance, cfg.CurrencyPairs[0].Providers[1])
	require.Equal(t, "2", cfg.Deviations[0].Threshold)
	require.Equal(t, "USDT", cfg.Deviations[0].Base)
	require.Equal(t, "1.5", cfg.Deviations[1].Threshold)
	require.Equal(t, "ATOM", cfg.Deviations[1].Base)
}

func TestParseConfig_Invalid_Deviations(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "price-feeder.toml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := []byte(`
[server]
listen_addr = "0.0.0.0:99999"
read_timeout = "20s"
verbose_cors = true
write_timeout = "20s"

[[deviation_thresholds]]
base = "USDT"
threshold = "4.0"

[[deviation_thresholds]]
base = "ATOM"
threshold = "1.5"

[[currency_pairs]]
base = "ATOM"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "UMEE"
quote = "USDT"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[[currency_pairs]]
base = "USDT"
quote = "USD"
providers = [
	"kraken",
	"binance",
	"huobi"
]

[telemetry]
service_name = "price-feeder"
enabled = true
enable_hostname = true
enable_hostname_label = true
enable_service_label = true
prometheus_retention = 120
global_labels = [["chain-id", "umee-local-testnet"]]
`)
	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	_, err = config.ParseConfig(tmpFile.Name())
	require.Error(t, err)
}
