"""
Synthetic data generator for Acme Marketplace.

Intentionally generates imperfect data to demonstrate realistic operational
data problems: nulls, duplicates, invalid timestamps, negative inventory,
zero quantities, extreme price changes, missing fields, and schema drift.
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta, timezone

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "samples")

PRODUCTS = [f"SKU-{i:03d}" for i in range(1, 21)]
CHANNELS = ["web", "mobile", "store", "partner"]
WAREHOUSES = ["tokyo", "osaka", "nagoya", "fukuoka"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _random_timestamp(days_back: int = 30) -> str:
    dt = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return dt.isoformat().replace("+00:00", "Z")


def _inject_order_defects(order: dict, defect_type: str) -> dict:
    if defect_type == "null_order_id":
        order["order_id"] = None
    elif defect_type == "null_product_id":
        order["product_id"] = None
    elif defect_type == "zero_quantity":
        order["quantity"] = 0
    elif defect_type == "negative_quantity":
        order["quantity"] = -random.randint(1, 5)
    elif defect_type == "zero_price":
        order["unit_price"] = 0
    elif defect_type == "invalid_timestamp":
        order["ordered_at"] = "NOT-A-DATE"
    elif defect_type == "missing_channel":
        del order["channel"]
    elif defect_type == "schema_drift":
        order["extra_field"] = "unexpected_value"
        del order["unit_price"]
    return order


def generate_orders(n: int = 200) -> list[dict]:
    orders = []
    order_id_counter = 10001

    defect_pool = [
        "null_order_id",
        "null_product_id",
        "zero_quantity",
        "negative_quantity",
        "zero_price",
        "invalid_timestamp",
        "missing_channel",
        "schema_drift",
    ]

    for i in range(n):
        order = {
            "order_id": f"ORD-{order_id_counter}",
            "product_id": random.choice(PRODUCTS),
            "channel": random.choice(CHANNELS),
            "quantity": random.randint(1, 10),
            "unit_price": random.choice([800, 1200, 1500, 2000, 3500, 5000, 8000]),
            "ordered_at": _random_timestamp(30),
        }
        order_id_counter += 1

        roll = random.random()
        if roll < 0.08:
            defect = random.choice(defect_pool)
            order = _inject_order_defects(order, defect)
        elif roll < 0.13:
            # duplicate order_id (use a recent id)
            if orders:
                order["order_id"] = random.choice(orders[-20:])["order_id"]

        orders.append(order)

    return orders


def generate_inventory(n_records: int = 80) -> list[dict]:
    rows = []
    for _ in range(n_records):
        product = random.choice(PRODUCTS)
        warehouse = random.choice(WAREHOUSES)
        qty = random.randint(-5, 300)
        ts = _random_timestamp(3)

        row = {
            "product_id": product,
            "warehouse": warehouse,
            "quantity": qty,
            "updated_at": ts,
        }

        roll = random.random()
        if roll < 0.05:
            row["product_id"] = None
        elif roll < 0.08:
            row["updated_at"] = "BAD-TIMESTAMP"
        elif roll < 0.10:
            del row["warehouse"]

        rows.append(row)

    return rows


def generate_price_events(n: int = 60) -> list[dict]:
    events = []
    for _ in range(n):
        product = random.choice(PRODUCTS)
        old_price = random.choice([800, 1200, 1500, 2000, 3500])

        # normal change most of the time
        roll = random.random()
        if roll < 0.15:
            # extreme spike
            new_price = int(old_price * random.uniform(1.35, 2.5))
        elif roll < 0.25:
            # extreme drop
            new_price = int(old_price * random.uniform(0.3, 0.65))
        else:
            # minor change
            new_price = int(old_price * random.uniform(0.9, 1.15))

        event = {
            "product_id": product,
            "old_price": old_price,
            "new_price": new_price,
            "changed_at": _random_timestamp(30),
            "source": "pricing-service",
        }

        defect_roll = random.random()
        if defect_roll < 0.04:
            event["product_id"] = None
        elif defect_roll < 0.07:
            event["old_price"] = 0
        elif defect_roll < 0.09:
            event["changed_at"] = "INVALID"

        events.append(event)

    return events


def write_orders(orders: list[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for order in orders:
            f.write(json.dumps(order, ensure_ascii=False) + "\n")


def write_inventory(rows: list[dict], path: str) -> None:
    if not rows:
        return
    fieldnames = ["product_id", "warehouse", "quantity", "updated_at"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_price_events(events: list[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    orders = generate_orders(200)
    inventory = generate_inventory(80)
    price_events = generate_price_events(60)

    write_orders(orders, os.path.join(OUTPUT_DIR, "orders.jsonl"))
    write_inventory(inventory, os.path.join(OUTPUT_DIR, "inventory.csv"))
    write_price_events(price_events, os.path.join(OUTPUT_DIR, "price_events.jsonl"))

    print(f"Generated {len(orders)} orders -> data/samples/orders.jsonl")
    print(f"Generated {len(inventory)} inventory rows -> data/samples/inventory.csv")
    print(f"Generated {len(price_events)} price events -> data/samples/price_events.jsonl")


if __name__ == "__main__":
    main()
