# Oracle Price Feeder Dockerfile

## Build Docker Image
Change `VERSION` to the release you want to build.

```bash
VERSION=master
git clone https://github.com/Team-Kujira/oracle-price-feeder.git
cd oracle-price-feeder/dockerfile || exit
docker build --build-arg VERSION=$VERSION -t price-feeder:latest .
```

## Create `config.toml`

```bash
sudo tee config.toml <<EOF
gas_adjustment = 1.5
gas_prices = "0.00125ukuji"
enable_server = true
enable_voter = true
provider_timeout = "500ms"

[server]
listen_addr = "0.0.0.0:7171"
read_timeout = "20s"
verbose_cors = true
write_timeout = "20s"

[[deviation_thresholds]]
base = "USDT"
threshold = "2"

[account]
address = "kujira..."
chain_id = "kaiyo-1"
validator = "kujiravaloper..."
prefix = "kujira"

[keyring]
backend = "file"
dir = "/root/.kujira"

[rpc]
grpc_endpoint = "localhost:9090"
rpc_timeout = "100ms"
tmrpc_endpoint = "http://localhost:26657"

[telemetry]
enable_hostname = true
enable_hostname_label = true
enable_service_label = true
enabled = true
global_labels = [["chain_id", "kaiyo-1"]]
service_name = "price-feeder"
type = "prometheus"
prometheus_retention = 120

[[provider_endpoints]]
name = "binance"
rest = "https://api1.binance.com"
websocket = "stream.binance.com:9443"

[[currency_pairs]]
base = "ATOM"
providers = [
  "binance",
  "kraken",
  "osmosis",
]
quote = "USD"
EOF
```

## Create `client.toml`

```bash
sudo tee client.toml <<EOF
chain-id = "kaiyo-1"
keyring-backend = "file"
output = "text"
node = "tcp://localhost:26657"
broadcast-mode = "sync"
EOF
```

## Run Test Shell
```bash
docker run \
--env PRICE_FEEDER_PASS=password \
-v "$PWD"/config.toml:/root/price-feeder/config.toml \
-v "$PWD"/client.toml:/root/.kujira/config/client.toml \
-it --entrypoint /bin/sh oracle-price-feeder
```


## Run Docker Image
```bash
docker run \
--env PRICE_FEEDER_PASS=password \
-v "$PWD"/config.toml:/root/price-feeder/config.toml \
-v "$PWD"/client.toml:/root/.kujira/config/client.toml \
-it oracle-price-feeder /root/price-feeder/config.toml
```