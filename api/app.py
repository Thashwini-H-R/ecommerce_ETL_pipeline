"""Simple FastAPI app to expose warehouse data for dashboards and downstream tools.

Endpoints:
 - GET /orders       -> query orders with optional date range, customer_id, limit
 - GET /customers    -> query customers with optional limit
 - GET /transactions -> query transactions with optional date range, limit
 - GET /metrics/orders_per_day -> orders count by day between dates

Uses psycopg2 connection via loader.get_conn() to query the Postgres/TimescaleDB warehouse.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

import loader

app = FastAPI(title="ecommerce-warehouse-api")


class OrderOut(BaseModel):
    order_id: str
    order_date: Optional[datetime]
    customer_id: Optional[str]
    currency: Optional[str]
    total_amount: Optional[float]


class CustomerOut(BaseModel):
    customer_id: str
    email: Optional[str]
    name: Optional[str]
    created_at: Optional[datetime]
    total_lifetime_value: Optional[float]


class TransactionOut(BaseModel):
    transaction_id: str
    transaction_date: Optional[datetime]
    order_id: Optional[str]
    customer_id: Optional[str]
    amount: Optional[float]
    currency: Optional[str]
    status: Optional[str]


def _rows_to_dicts(cur) -> List[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


@app.get("/orders", response_model=List[OrderOut])
def get_orders(
    start: Optional[str] = Query(None, description="ISO start date (inclusive)"),
    end: Optional[str] = Query(None, description="ISO end date (inclusive)"),
    customer_id: Optional[str] = None,
    limit: int = 100,
):
    q = "SELECT order_id, order_date, customer_id, currency, total_amount FROM orders_fact"
    wheres = []
    params = []
    if start:
        wheres.append("order_date >= %s")
        params.append(start)
    if end:
        wheres.append("order_date <= %s")
        params.append(end)
    if customer_id:
        wheres.append("customer_id = %s")
        params.append(customer_id)

    if wheres:
        q += " WHERE " + " AND ".join(wheres)
    q += " ORDER BY order_date DESC NULLS LAST LIMIT %s"
    params.append(limit)

    try:
        with loader.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, tuple(params))
                rows = _rows_to_dicts(cur)
        return rows
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/customers", response_model=List[CustomerOut])
def get_customers(limit: int = 100):
    q = "SELECT customer_id, email, name, created_at, total_lifetime_value FROM customers_dim ORDER BY created_at DESC NULLS LAST LIMIT %s"
    try:
        with loader.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (limit,))
                rows = _rows_to_dicts(cur)
        return rows
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/transactions", response_model=List[TransactionOut])
def get_transactions(start: Optional[str] = None, end: Optional[str] = None, limit: int = 100):
    q = "SELECT transaction_id, transaction_date, order_id, customer_id, amount, currency, status FROM transactions_fact"
    wheres = []
    params = []
    if start:
        wheres.append("transaction_date >= %s")
        params.append(start)
    if end:
        wheres.append("transaction_date <= %s")
        params.append(end)
    if wheres:
        q += " WHERE " + " AND ".join(wheres)
    q += " ORDER BY transaction_date DESC NULLS LAST LIMIT %s"
    params.append(limit)

    try:
        with loader.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, tuple(params))
                rows = _rows_to_dicts(cur)
        return rows
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/metrics/orders_per_day")
def orders_per_day(start: Optional[str] = None, end: Optional[str] = None):
    q = "SELECT date_trunc('day', order_date) as day, count(*) as orders FROM orders_fact"
    wheres = []
    params = []
    if start:
        wheres.append("order_date >= %s")
        params.append(start)
    if end:
        wheres.append("order_date <= %s")
        params.append(end)
    if wheres:
        q += " WHERE " + " AND ".join(wheres)
    q += " GROUP BY day ORDER BY day"

    try:
        with loader.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, tuple(params))
                rows = _rows_to_dicts(cur)
        return rows
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("API_PORT", 8000)))
