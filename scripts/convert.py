#!/usr/bin/env python3

import argparse
import json
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    return parser.parse_args()


def main():
    args = parse_args()

    out = []

    for line in open(args.csv).readlines()[1:]:
        line = line.strip()
        parts = line.split(";")
        out.append({
            "time": datetime.utcfromtimestamp(int(parts[0])).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            "price": parts[1].replace(",", "."),
            "volume": parts[2].replace(",", "."),
        })

    print(json.dumps(out))


if __name__ == "__main__":
    main()
