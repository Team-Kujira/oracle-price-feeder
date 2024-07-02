#!/usr/bin/env python3

import argparse
import requests
import sys

ADDRESSES = {
    "camelotv3": "0x22427d20480de289795ca29c3adddb57a568e285"  # KUJIWETH
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", help="provider to query against",
                        choices=["camelotv3"])
    parser.add_argument("endpoint", help="rpc endpoint")
    return parser.parse_args()


def rpc_call(endpoint, method, params):
    data = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    }
    response = requests.post(endpoint, json=data)

    response.raise_for_status()

    data = response.json()
    error = data.get("error")
    if error:
        print(error)
        sys.exit(1)

    return response.json().get("result")


def get_height(endpoint):
    height = rpc_call(endpoint, "eth_blockNumber", [])
    if not height:
        print("no height found")
        return None

    return int(height, 0)


def get_logs(endpoint, height, address):
    params = {
        "fromBlock": hex(height-2000),
        "toBlock": hex(height),
        "address": [address],
        "topics": [],
    }

    return rpc_call(endpoint, "eth_getLogs", [params])


def main():
    args = parse_args()

    height = get_height(args.endpoint)

    address = ADDRESSES.get(args.provider)
    if not address:
        print(f"no address found for provider {args.provider}")

    logs = get_logs(args.endpoint, height, address)
    if logs == None:
        print("this shouldn't happen")

    print("ok")


if __name__ == "__main__":
    main()
