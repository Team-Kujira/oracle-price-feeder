#!/usr/bin/env python3

import requests
import json
import logging


def main():
    response = requests.get("https://api.kraken.com/0/public/AssetPairs")
    if response.status_code != 200:
        logging.error("Failed to get asset pairs")
        return
    
    data = response.json()
    pairs = data.get("result", {})

    mapping = {}

    for symbol, pair in pairs.items():
        wsname = pair["wsname"]
        base, quote = wsname.split("/")
        if base + quote != symbol:
            mapping[base + quote] = symbol
    
    print(json.dumps(mapping))

if __name__ == "__main__":
    main()