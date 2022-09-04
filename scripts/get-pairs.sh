#!/bin/bash
# Helper script to find providers that support a specific denom
# Requires jq to be installed and accessible in PATH
# Example command: ./get-pairs.sh BTC USD

osmosisURL="https://api-osmosis.imperator.co/pairs/v1/summary"
okxURL="https://www.okx.com/api/v5/market/tickers?instType=SPOT"
mexcURL="https://www.mexc.com/open/api/v2/market/ticker"
krakenURL="https://api.kraken.com/0/public/AssetPairs"
huobiURL="https://api.huobi.pro/market/tickers"
gateURL="https://api.gateio.ws/api/v4/spot/currency_pairs"
finURL="https://api.kujira.app/api/coingecko/pairs"
coinbaseURL="https://api.exchange.coinbase.com/products"
binanceURL="https://api1.binance.com/api/v3/ticker/price"
binanceusURL="https://api.binance.us/api/v3/ticker/price"

query_denom () {
    echo "Osmosis:"
    curl -s $osmosisURL | jq .'data[] | "  " + .base_symbol + "-" + .quote_symbol' | tr [:lower:] [:upper:] | tr -d \" | grep $1 | grep $2
    echo ""
    echo "OKX:"
    curl -s $okxURL | jq .'data[] | "  " + .instId' | tr -d \" | grep $1 | grep $2
    echo ""
    echo "MEXC:"
    curl -s $mexcURL | jq .'data[] | "  " + .symbol' | tr -d \" | grep $1 | grep $2
    echo ""
    echo "Kraken:"
    curl -s $krakenURL | jq .'result[] | "  " + .wsname' | tr -d \" | tr / - | grep $1 | grep $2
    echo ""
    echo "Huobi:"
    curl -s $huobiURL | jq .'data[] | "  " + .symbol' | tr [:lower:] [:upper:] | tr -d \" | grep $1 | grep $2
    echo ""
    echo "Gate:"
    curl -s $gateURL | jq .'[] | "  " + .base + "-" + .quote' | tr -d \" | grep $1 | grep $2
    echo ""
    echo "Fin:"
    curl -s $finURL | jq .'pairs[] | "  " + .base + "-" + .target' | tr -d \" | grep $1 | grep $2
    echo ""
    echo "Coinbase:"
    curl -s $coinbaseURL | jq .'[] | "  " + .id' | tr -d \" | grep $1 | grep $2
    echo ""
    echo "Binance.com:"
    curl -s $binanceURL | jq .'[] | "  " + .symbol' | tr -d \" | grep $1 | grep $2
    echo ""
    echo "Binance.us:"
    curl -s $binanceusURL | jq .'[] | "  " + .symbol' | tr -d \" | grep $1 | grep $2
    echo ""
}


if [ $# -lt 2 ]
then
    if [ $# -eq 1 ]
    then
        echo "No quote denom provided - assuming USD"
        echo "Next time try something like: ./get-pairs.sh BTC USD"
        query_denom $1 USD
    else
        echo "No input arguments provided"
        echo "Try something like: ./get-pairs.sh BTC USD"
        exit 1
    fi
else
    query_denom $1 $2
fi