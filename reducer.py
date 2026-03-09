#!/usr/bin/env python3
"""
Hadoop Streaming Reducer — Retail Transactions Dataset
Task: Total revenue and transaction count per city.

Input  (stdin):  sorted mapper output — city TAB total_cost
Output (stdout): city TAB total_revenue TAB transaction_count
"""

import sys
import io

sys.stdin  = io.TextIOWrapper(sys.stdin.buffer,  encoding="utf-8")
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)


def emit(city, total_revenue, count):
    print(f"{city}\t{total_revenue:.2f}\t{count}")


def parse_line(line):
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 2:
        sys.stderr.write(f"SKIPPED malformed input: {line!r}\n")
        return None
    city, raw_cost = parts
    try:
        return city, float(raw_cost)
    except ValueError:
        sys.stderr.write(f"SKIPPED non-numeric cost: {line!r}\n")
        return None


def main():
    print("city\ttotal_revenue\ttransaction_count")

    current_city  = None
    total_revenue = 0.0
    count         = 0

    for line in sys.stdin:
        parsed = parse_line(line)
        if parsed is None:
            continue

        city, cost = parsed

        if city == current_city:
            total_revenue += cost
            count         += 1
        else:
            if current_city is not None:
                emit(current_city, total_revenue, count)
            current_city  = city
            total_revenue = cost
            count         = 1

    if current_city is not None:
        emit(current_city, total_revenue, count)


if __name__ == "__main__":
    main()