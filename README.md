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
- [Phemex](https://phemex.com)
- [Poloniex](https://poloniex.com)
- [Pyth](https://pyth.network)
- [UniswapV3](https://app.uniswap.org)
- [XT.COM](https://www.xt.com/en)

## Usage

The `price-feeder` tool runs off of a single configuration file. This configuration
file defines what exchange rates to fetch and what providers to get them from.
In addition, it defines the oracle's keyring and feeder account information.
The keyring's password is defined via environment variables or user input.
More information on the keyring can be found [here](#keyring)
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

### `rpc`

The `rpc` section contains the Tendermint and Cosmos application gRPC endpoints.
These endpoints are used to query for on-chain data that pertain to oracle
functionality and for broadcasting signed pre-vote and vote oracle messages.

### `telemetry`

A set of options for the application's telemetry, which is disabled by default. An in-memory sink is the default, but Prometheus is also supported. We use the [cosmos sdk telemetry package](https://github.com/cosmos/cosmos-sdk/blob/main/docs/core/telemetry.md).

### `account`

The `account` section contains the oracle's feeder and validator account information.
These are used to sign and populate data in pre-vote and vote oracle messages.

### `keyring`

The `keyring` section contains Keyring related material used to fetch the key pair
associated with the oracle account that signs pre-vote and vote oracle messages.

### `healthchecks`

The `healthchecks` section defines optional healthcheck endpoints to ping on successful
oracle votes. This provides a simple alerting solution which can integrate with a service
like [healthchecks.io](https://healthchecks.io). It's recommended to configure additional
monitoring since third-party services can be unreliable.

### `deviation_thresholds`

Deviation thresholds allow validators to set a custom amount of standard deviations around the median which is helpful if any providers become faulty. It should be noted that the default for this option is 1 standard deviation.

### `provider_min_overrides`

This option allows validators to set the minimum prices sources needed for specific assets. This might be necessary, if there are less than three providers available for a certain asset.

### `provider_endpoints`

The provider_endpoints option enables validators to setup their own API endpoints for a given provider.

### `contract_addresses`

The `contract_addresses` sections contain a mapping of base/denom pair to the pool addresses of supported decentralized exchanges.

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
market data. Prices per exchange rate are submitted on-chain via pre-vote and
vote messages using a volume-weighted average price (VWAP).

## Keyring

Our keyring must be set up to sign transactions before running the price feeder.
Additional info on the different keyring modes is available [here](https://docs.cosmos.network/master/run-node/keyring.html).
**Please note that the `test` and `memory` modes are only for testing purposes.**
**Do not use these modes for running the price feeder against mainnet.**

### Setup

The keyring `dir` and `backend` are defined in the config file.
You may use the `PRICE_FEEDER_PASS` environment variable to set up the keyring password.

Ex :
`export PRICE_FEEDER_PASS=keyringPassword`

If this environment variable is not set, the price feeder will prompt the user for input.
