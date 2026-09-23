#!/usr/bin/env python3
"""
Generate dummy customer + transaction CSV data for a PySpark join exercise.

Mirrors the PostgreSQL generator: same schema, same deliberate data-quality
problems, same "customers with no transactions" gap.

Output is written as multiple part files per dataset so Spark gets real
partitions to work with instead of one unsplittable blob.

Usage:
    python generate_csv.py [--customers N] [--transactions N] [--out DIR]
"""

import argparse
import os
import random

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Sandra", "Mark", "Ashley", "Donald", "Kimberly",
    "Steven", "Emily", "Paul", "Donna", "Andrew", "Michelle", "Joshua", "Carol",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]

DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "icloud.com", "aol.com", "protonmail.com", "mail.com",
]

PLACES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"),
    ("San Francisco", "CA"), ("Indianapolis", "IN"), ("Seattle", "WA"),
    ("Denver", "CO"), ("Boston", "MA"), ("Nashville", "TN"),
    ("Detroit", "MI"), ("Portland", "OR"), ("Memphis", "TN"),
    ("Las Vegas", "NV"), ("Atlanta", "GA"), ("Miami", "FL"),
    ("Minneapolis", "MN"), ("Tampa", "FL"), ("Pittsburgh", "PA"),
]

PRODUCTS = [
    ("Laptop", 1200.00), ("Smartphone", 850.00), ("Headphones", 120.00),
    ("Monitor", 300.00), ("Keyboard", 75.00), ("Mouse", 35.00),
    ("Webcam", 60.00), ("Tablet", 450.00), ("Printer", 210.00),
    ("External SSD", 140.00), ("Router", 95.00), ("Smart Watch", 320.00),
    ("Desk Lamp", 45.00), ("Office Chair", 260.00), ("Standing Desk", 540.00),
    ("USB-C Hub", 55.00), ("Microphone", 130.00), ("Speaker", 180.00),
    ("Graphics Card", 700.00), ("Power Bank", 40.00),
]

CUSTOMER_HEADER = "customer_id,customer_name,email,city,state,registration_date"
TRANSACTION_HEADER = "transaction_id,customer_id,product,transaction_date,quantity,amount"

# Epoch helpers: days since 1970-01-01, converted with datetime only at the end.
from datetime import date, timedelta

REG_START = date(2018, 1, 1)
TXN_START = date(2023, 1, 1)

# Precompute date strings so we format each one only once.
REG_DATES = [(REG_START + timedelta(days=d)).isoformat() for d in range(2901)]
TXN_DATES = [(TXN_START + timedelta(days=d)).isoformat() for d in range(1001)]


def q(value):
    """Quote a field so leading/trailing spaces and empties survive the round trip."""
    return '"' + value + '"'


def write_parts(path, header, rows_iter, total_rows, n_parts):
    """Split rows_iter across n_parts files, each with its own header row."""
    os.makedirs(path, exist_ok=True)
    per_part = -(-total_rows // n_parts)  # ceiling division
    written = 0
    part = 0
    fh = None
    try:
        for row in rows_iter:
            if fh is None or written >= per_part:
                if fh is not None:
                    fh.close()
                fname = os.path.join(path, "part-%05d.csv" % part)
                fh = open(fname, "w", encoding="utf-8", newline="\n")
                fh.write(header + "\n")
                part += 1
                written = 0
            fh.write(row)
            written += 1
    finally:
        if fh is not None:
            fh.close()
    return part


def gen_customers(n):
    """Yield customer CSV lines, including duplicates and dirty join keys."""
    rnd = random.Random(20260830)
    n_domains = len(DOMAINS)
    n_places = len(PLACES)
    n_first = len(FIRST_NAMES)
    n_last = len(LAST_NAMES)

    for i in range(1, n + 1):
        cid = "CUST-%07d" % i
        fn = FIRST_NAMES[rnd.randrange(n_first)]
        ln = LAST_NAMES[rnd.randrange(n_last)]
        name = fn + " " + ln
        email = "%s.%s%d@%s" % (fn.lower(), ln.lower(), i, DOMAINS[rnd.randrange(n_domains)])
        city, state = PLACES[rnd.randrange(n_places)]
        reg = REG_DATES[rnd.randrange(2901)]

        # Dirt: padded + lowercased join key on ~2% of rows.
        key = q("  " + cid.lower() + " ") if i % 50 == 7 else cid
        # Dirt: missing email on ~0.7%, missing city/state on ~0.5%.
        e = "" if i % 137 == 0 else email
        if i % 211 == 0:
            city, state = "", ""

        line = "%s,%s,%s,%s,%s,%s\n" % (key, name, e, city, state, reg)
        yield line

        # Dirt: exact duplicate row on ~1%.
        if i % 100 == 0:
            yield line


def gen_transactions(n, active_customers):
    """Yield transaction CSV lines, including duplicates, nulls and orphan keys."""
    rnd = random.Random(20260831)
    n_products = len(PRODUCTS)

    for i in range(1, n + 1):
        # Squaring the uniform draw skews purchases toward low-numbered
        # customers, producing a realistic long tail of heavy spenders.
        cust_n = 1 + int(active_customers * (rnd.random() ** 2))
        cid = "CUST-%07d" % cust_n

        product, unit_price = PRODUCTS[rnd.randrange(n_products)]
        qty = rnd.randrange(1, 6)
        amount = round(unit_price * qty * (0.90 + rnd.random() * 0.20), 2)
        txn_date = TXN_DATES[rnd.randrange(1001)]
        txn_id = "TXN-%09d" % i

        # Dirt applied to the join key, in priority order.
        if i % 500 == 0:
            key = ""                                    # null join key
        elif i % 977 == 0:
            key = q("")                                 # blank-but-present join key
        elif i % 400 == 13:
            key = "CUST-9%06d" % (i % 100000)           # orphan: no such customer
        elif i % 50 == 23:
            key = q(" " + cid.lower() + "  ")           # padded + lowercased
        else:
            key = cid

        # Dirt: missing measures, negative amounts, zero quantities.
        qty_out = "" if i % 733 == 0 else ("0" if i % 1699 == 0 else str(qty))
        if i % 811 == 0:
            amt_out = ""
        elif i % 1301 == 0:
            amt_out = "%.2f" % -amount
        else:
            amt_out = "%.2f" % amount

        line = "%s,%s,%s,%s,%s,%s\n" % (txn_id, key, product, txn_date, qty_out, amt_out)
        yield line

        # Dirt: exact duplicate row on ~1%.
        if i % 100 == 0:
            yield line


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=600000)
    ap.add_argument("--transactions", type=int, default=2900000)
    ap.add_argument("--active-fraction", type=float, default=0.85,
                    help="fraction of customers that ever transact; the rest have none")
    ap.add_argument("--customer-parts", type=int, default=4)
    ap.add_argument("--transaction-parts", type=int, default=8)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()

    active = int(args.customers * args.active_fraction)

    cust_rows = args.customers + args.customers // 100
    txn_rows = args.transactions + args.transactions // 100

    c_parts = write_parts(
        os.path.join(args.out, "customers"), CUSTOMER_HEADER,
        gen_customers(args.customers), cust_rows, args.customer_parts)

    t_parts = write_parts(
        os.path.join(args.out, "transactions"), TRANSACTION_HEADER,
        gen_transactions(args.transactions, active), txn_rows, args.transaction_parts)

    print("customers    : %d rows across %d part files" % (cust_rows, c_parts))
    print("transactions : %d rows across %d part files" % (txn_rows, t_parts))
    print("customers with zero transactions by construction: %d"
          % (args.customers - active))


if __name__ == "__main__":
    main()
