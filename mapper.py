#!/usr/bin/env python3
"""
Hadoop Streaming Mapper — Retail Transactions Dataset
Task: Total revenue and transaction count per city.
Dataset: Retail Transactions Dataset (1,000,000 rows)
Source: kaggle.com/datasets/prasad22/retail-transactions-dataset

Column indices:
  5 → Total_Cost  (float, USD)
  7 → City        (string)
"""

import sys
import csv
import io

sys.stdin  = io.TextIOWrapper(sys.stdin.buffer,  encoding="utf-8")
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)


def parse_cost(raw):
    try:
        val = float(raw.strip())
        return val if val >= 0.0 else None
    except ValueError:
        return None


def extract_city(raw):
    val = raw.strip()
    return val if val else None


def main():
    reader = csv.reader(sys.stdin)
    for line_num, row in enumerate(reader):
        if line_num == 0:
            continue
        if len(row) < 8:
            sys.stderr.write(f"SKIPPED short row at line {line_num + 1}\n")
            continue

        cost = parse_cost(row[5])
        city = extract_city(row[7])

        if cost is None:
            sys.stderr.write(f"SKIPPED invalid cost at line {line_num + 1}: '{row[5]}'\n")
            continue
        if city is None:
            sys.stderr.write(f"SKIPPED missing city at line {line_num + 1}\n")
            continue

        print(f"{city}\t{cost:.2f}")


if __name__ == "__main__":
    main()