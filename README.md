# Oracle Price Feeder

This is a standalone version of [Umee's fantastic work](https://github.com/umee-network/umee/tree/main/price-feeder) migrating [Terra's oracle-feeder](https://github.com/terra-money/oracle-feeder) app to Go, and integrating it more closely with the Cosmos SDK.

## Providers

The list of current supported providers:

- [Astroport](https://astroport.fi/en)
- [Binance](https://www.binance.com/en)
- [BinanceUS](https://www.binance.us)
- [Bitfinex](https://www.bitfinex.com)
- [Bitget](https://www.bitget.com/en/)
- [Bitmart](https://www.bitmart.com/en-US)
- [Bitstamp](https://www.bitstamp.net)
- [Bybit](https://www.bybit.com/en-US/)
- [Camelot DEX](https://excalibur.exchange)
- [Coinbase](https://www.coinbase.com/)
- [Crypto.com](https://crypto.com/eea)
- [Curve](https://curve.fi)
- [FIN](https://fin.kujira.app)
- [Gate.io](https://www.gate.io)
- [HitBTC](https://hitbtc.com)
- [Huobi](https://www.huobi.com/en-us/)
- [Kraken](https://www.kraken.com/en-us/)
- [Kucoin](https://www.kucoin.com)
- [LBank](https://www.lbank.com)
- [MEXC](https://www.mexc.com/)
- [Okx](https://www.okx.com/)
- [Osmosis](https://app.osmosis.zone/)
- [PancakeSwap (Ethereum)](https://pancakeswap.finance)
- [Phemex](https://phemex.com)
- [Poloniex](https://poloniex.com)
- [Pyth](https://pyth.network)
- [UniswapV3](https://app.uniswap.org)
- [WhiteWhale](https://whitewhale.money)
- [XT.COM](https://www.xt.com/en)

## Usage

The `price-feeder` tool runs off of a single configuration file. This configuration
file defines what exchange rates to fetch and what providers to get them from.
Please see the [example configuration](config.example.toml) for more details.

```shell
price-feeder /path/to/price_feeder_config.toml
```

## Installation

An extensive installation guide can be found [here](https://docs.kujira.app/validators/run-a-node/oracle-price-feeder).

## Configuration

### `server`

The `server` section contains configuration pertaining to the API served by the
`price-feeder` process such the listening address and various HTTP timeouts.

### `telemetry`

A set of options for the application's telemetry, which is disabled by default. An in-memory sink is the default, but Prometheus is also supported. We use the [cosmos sdk telemetry package](https://github.com/cosmos/cosmos-sdk/blob/main/docs/core/telemetry.md).

### `healthchecks`

The `healthchecks` section defines optional healthcheck endpoints to ping on successful
oracle votes. This provides a simple alerting solution which can integrate with a service
like [healthchecks.io](https://healthchecks.io). It's recommended to configure additional
monitoring since third-party services can be unreliable.

### `deviation_thresholds`

Deviation thresholds allow validators to set a custom amount of standard deviations around the median which is helpful if any providers become faulty. It should be noted that the default for this option is 1 standard deviation.

```toml
[[deviation_thresholds]]
base = "USDT"
threshold = "2"
```

### `provider_min_overrides`

This option allows validators to set the minimum prices sources needed for specific assets. This might be necessary, if there are less than three providers available for a certain asset.

```toml
[[provider_min_overrides]]
denoms = ["QCKUJI", "QCMNTA"]
providers = 1
```

### `url_set`

Url sets are named arrays of endpoint urls, that can be reused in endpoint configurations.

```toml
[url_set.rest_kujira]
urls = [
  "https://rest.cosmos.directory/kujira",
]

[[provider_endpoints]]
name = "finv2"
url_set = "rest_kujira"
```

### `provider_endpoints`

The provider_endpoints option enables validators to setup their own API endpoints for a given provider.

```toml
[[provider_endpoints]]
name = "finv2"
urls = [
  "https://rest.cosmos.directory/kujira",
]
```

### `contract_addresses`

The `contract_addresses` sections contain a mapping of base/denom pair to the pool addresses of supported decentralized exchanges.

```toml
[contract_addresses.finv2]
KUJIUSDC = "kujira14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9sl4e867"
MNTAUSDC = "kujira1ws9w7wl68prspv3rut3plv8249rm0ea0kk335swye3sl2slld4lqdmc0lv"
```

### `currency_pairs`

The `currency_pairs` sections contains one or more exchange rates along with the
providers from which to get market data from. It is important to note that the
providers supplied in each `currency_pairs` must support the given exchange rate.

For example, to get multiple price points on ATOM, you could define `currency_pairs`
as follows:

```toml
[[currency_pairs]]
base = "ATOM"
quote = "USDT"
providers = [
  "binance",
]

[[currency_pairs]]
base = "ATOM"
quote = "USD"
providers = [
  "kraken",
  "osmosis",
]
```

Providing multiple providers is beneficial in case any provider fails to return
market data or reports a price that deviates too much and should be considered wrong. Prices per exchange rate are submitted on-chain via pre-vote and
vote messages using a volume-weighted average price (VWAP).

### `provider_weight`

Provider weight sets the volume for the given providers of a specific denom. This can be used manually set the impact of specific providers during the vwap calculation or create some kind of ordered failover mechanism.

```toml
[provider_weight.STATOM]
provider1 = 100
provider2 = 0.001
provider3 = 0
```

In this example the resulting price will be following provider1 as long as it is available (100k times more weight than provider2). If provider1 fails, the resulting price will follow provider2, and if that fails it too, the resulting price is the one reported by provider3. All assuming the deviation of the all prices are within the configured range.
