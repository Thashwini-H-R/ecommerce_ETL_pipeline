"""Batch loader to insert/upsert cleaned/transformed data into the warehouse.

This file uses psycopg2 and psycopg2.extras.execute_values for efficient batch upserts.
Configure DB connection via environment variables:
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

Functions:
  - get_conn()
  - upsert_customers(list[dict])
  - upsert_products(list[dict])
  - upsert_orders(list[dict])
  - upsert_transactions(list[dict])

The loader performs INSERT ... ON CONFLICT DO UPDATE to keep dimensions/facts up-to-date.
"""
from __future__ import annotations

import json
import os
from typing import Dict, Iterable, List, Sequence

import psycopg2
import psycopg2.extras as extras


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ.get("PG_DB", "airflow"),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
    )


def _bulk_upsert(table: str, cols: Sequence[str], rows: Iterable[Sequence], conflict_cols: Sequence[str], update_cols: Sequence[str]):
    """Generic bulk upsert using psycopg2.extras.execute_values.

    - table: target table name
    - cols: list of column names in order for values
    - rows: iterable of tuples corresponding to cols
    - conflict_cols: columns to use in ON CONFLICT (e.g., primary key)
    - update_cols: columns to update on conflict
    """
    cols_sql = ",".join(cols)
    conflict_sql = ",".join(conflict_cols)
    # build SET clause: col = EXCLUDED.col
    set_sql = ",".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

    insert_sql = f"INSERT INTO {table} ({cols_sql}) VALUES %s ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_sql};"

    with get_conn() as conn:
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_sql, rows, template=None, page_size=1000)
        conn.commit()


def upsert_customers(customers: List[Dict]):
    """Upsert customers into customers_dim.

    Expected dict keys: customer_id, email, name, created_at (iso), last_order_id, total_lifetime_value, metadata (dict)
    """
    cols = ["customer_id", "email", "name", "created_at", "last_order_id", "total_lifetime_value", "metadata"]
    rows = []
    for c in customers:
        rows.append((
            c.get("customer_id"),
            c.get("email"),
            c.get("name"),
            c.get("created_at"),
            c.get("last_order_id"),
            c.get("total_lifetime_value"),
            json.dumps(c.get("metadata", {})),
        ))

    _bulk_upsert(
        table="customers_dim",
        cols=cols,
        rows=rows,
        conflict_cols=("customer_id",),
        update_cols=("email", "name", "created_at", "last_order_id", "total_lifetime_value", "metadata"),
    )


def upsert_products(products: List[Dict]):
    cols = ["product_id", "sku", "name", "category", "list_price", "active", "metadata"]
    rows = []
    for p in products:
        rows.append((
            p.get("product_id"),
            p.get("sku"),
            p.get("name"),
            p.get("category"),
            p.get("list_price"),
            p.get("active", True),
            json.dumps(p.get("metadata", {})),
        ))

    _bulk_upsert(
        table="products_dim",
        cols=cols,
        rows=rows,
        conflict_cols=("product_id",),
        update_cols=("sku", "name", "category", "list_price", "active", "metadata"),
    )


def upsert_orders(orders: List[Dict]):
    """Upsert orders into orders_fact.

    Expected keys: order_id, order_date (iso), customer_id, currency, total_amount, subtotal, tax_amount,
    shipping_amount, item_count, shipping_address (dict), billing_address (dict), line_items (list), raw_payload (dict)
    """
    cols = [
        "order_id",
        "order_date",
        "customer_id",
        "currency",
        "total_amount",
        "subtotal",
        "tax_amount",
        "shipping_amount",
        "item_count",
        "shipping_address",
        "billing_address",
        "line_items",
        "raw_payload",
        "updated_at",
    ]
    rows = []
    for o in orders:
        rows.append((
            o.get("order_id"),
            o.get("order_date"),
            o.get("customer_id"),
            o.get("currency"),
            o.get("total_amount"),
            o.get("subtotal"),
            o.get("tax_amount"),
            o.get("shipping_amount"),
            o.get("item_count"),
            json.dumps(o.get("shipping_address") or {}),
            json.dumps(o.get("billing_address") or {}),
            json.dumps(o.get("line_items") or []),
            json.dumps(o.get("raw_payload") or {}),
            o.get("updated_at") or o.get("order_date"),
        ))

    _bulk_upsert(
        table="orders_fact",
        cols=cols,
        rows=rows,
        conflict_cols=("order_id",),
        update_cols=("order_date", "customer_id", "currency", "total_amount", "subtotal", "tax_amount", "shipping_amount", "item_count", "shipping_address", "billing_address", "line_items", "raw_payload", "updated_at"),
    )


def upsert_transactions(transactions: List[Dict]):
    cols = [
        "transaction_id",
        "transaction_date",
        "order_id",
        "customer_id",
        "payment_provider",
        "amount",
        "currency",
        "status",
        "raw_payload",
        "updated_at",
    ]
    rows = []
    for t in transactions:
        rows.append((
            t.get("transaction_id"),
            t.get("transaction_date"),
            t.get("order_id"),
            t.get("customer_id"),
            t.get("payment_provider"),
            t.get("amount"),
            t.get("currency"),
            t.get("status"),
            json.dumps(t.get("raw_payload") or {}),
            t.get("updated_at") or t.get("transaction_date"),
        ))

    _bulk_upsert(
        table="transactions_fact",
        cols=cols,
        rows=rows,
        conflict_cols=("transaction_id",),
        update_cols=("transaction_date", "order_id", "customer_id", "payment_provider", "amount", "currency", "status", "raw_payload", "updated_at"),
    )


__all__ = [
    "get_conn",
    "upsert_customers",
    "upsert_products",
    "upsert_orders",
    "upsert_transactions",
]


if __name__ == "__main__":
    # Quick smoke test: import and print function names. Does not connect to DB.
    print("loader module loaded. Available:", __all__)
