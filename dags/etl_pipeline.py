"""Airflow DAG: ETL pipeline orchestration (ingest -> transform -> load).

This DAG is intentionally permissive: it detects available sources via environment variables
and processes any files written to the `staging` directory. It uses PythonOperators to call
local functions and logs/attempts notifications on failures.

Configure via environment variables:
  - STAGING_DIR (default ./staging)
  - SCHEDULE (cron or @daily, default '@daily')
  - SHOPIFY_SHOP_NAME, SHOPIFY_ACCESS_TOKEN
  - WOOCOMMERCE_BASE, WOOCOMMERCE_KEY, WOOCOMMERCE_SECRET
  - STRIPE_API_KEY
  - PAYPAL_BASE, PAYPAL_ACCESS_TOKEN
  - INVENTORY_DB_DSN (optional)

Note: This DAG expects `ingest.Ingestor`, `transform` utilities, `loader` and `bookmarks` to exist
in the repo (they do in this project). It performs lightweight transformations and then calls
the loader's upsert functions to persist data to Postgres/TimescaleDB.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.email import send_email

from ingest import Ingestor
import transform
import loader
import bookmarks

logger = logging.getLogger("etl_dag")
logging.basicConfig(level=logging.INFO)


STAGING_DIR = os.environ.get("STAGING_DIR", "./staging")
SCHEDULE = os.environ.get("SCHEDULE", "@daily")


def _notify_failure(context: Dict[str, Any]) -> None:
    """Simple failure notifier. Uses Airflow's send_email if SMTP configured, else logs."""
    task = context.get("task_instance")
    msg = f"DAG {context.get('dag').dag_id} failed on task {task.task_id} at {datetime.utcnow().isoformat()}\n"
    msg += f"Log: {context.get('exception')}\n"

    to = os.environ.get("ALERT_EMAIL")
    if to:
        try:
            send_email(to=to, subject=f"ETL DAG failure: {context.get('dag').dag_id}", html_content=msg)
        except Exception:
            logger.exception("Failed sending failure email; logging instead")
            logger.error(msg)
    else:
        logger.error(msg)


def run_ingest(**kwargs):
    ing = Ingestor(staging_dir=STAGING_DIR)

    # detect and run connectors based on environment variables
    run_sources = []

    shop = os.environ.get("SHOPIFY_SHOP_NAME")
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN")
    if shop and token:
        logger.info("Running Shopify ingest for %s", shop)
        try:
            # read last bookmark (since_id) optionally
            last = bookmarks.get_bookmark("shopify")
            since_id = int(last) if last else None
            ing.ingest_shopify_orders(shop_name=shop, access_token=token, since_id=since_id)
            bookmarks.write_bookmark("shopify", datetime.utcnow().isoformat())
            run_sources.append("shopify")
        except Exception:
            logger.exception("Shopify ingest failed")

    wc_base = os.environ.get("WOOCOMMERCE_BASE")
    wc_k = os.environ.get("WOOCOMMERCE_KEY")
    wc_s = os.environ.get("WOOCOMMERCE_SECRET")
    if wc_base and wc_k and wc_s:
        logger.info("Running WooCommerce ingest for %s", wc_base)
        try:
            ing.ingest_woocommerce_orders(base_url=wc_base, consumer_key=wc_k, consumer_secret=wc_s)
            bookmarks.write_bookmark("woocommerce", datetime.utcnow().isoformat())
            run_sources.append("woocommerce")
        except Exception:
            logger.exception("WooCommerce ingest failed")

    stripe_key = os.environ.get("STRIPE_API_KEY")
    if stripe_key:
        logger.info("Running Stripe ingest")
        try:
            ing.ingest_stripe_charges(api_key=stripe_key)
            bookmarks.write_bookmark("stripe", datetime.utcnow().isoformat())
            run_sources.append("stripe")
        except Exception:
            logger.exception("Stripe ingest failed")

    paypal_base = os.environ.get("PAYPAL_BASE")
    paypal_token = os.environ.get("PAYPAL_ACCESS_TOKEN")
    if paypal_base and paypal_token:
        logger.info("Running PayPal ingest")
        try:
            ing.ingest_paypal_transactions(base_url=paypal_base, access_token=paypal_token)
            bookmarks.write_bookmark("paypal", datetime.utcnow().isoformat())
            run_sources.append("paypal")
        except Exception:
            logger.exception("PayPal ingest failed")

    # inventory DB snapshot (optional)
    inv_dsn = os.environ.get("INVENTORY_DB_DSN")
    if inv_dsn:
        logger.info("Running inventory DB ingest")
        try:
            ing.ingest_inventory_db(dsn=inv_dsn)
            bookmarks.write_bookmark("inventory", datetime.utcnow().isoformat())
            run_sources.append("inventory")
        except Exception:
            logger.exception("Inventory ingest failed")

    if not run_sources:
        logger.info("No ingest sources configured via environment variables. Skipping ingest.")


def run_transform(**kwargs):
    """Scan staging dir, run per-source parsers (Shopify/Stripe), run transforms, and upsert via loader."""
    customers: List[Dict[str, Any]] = []
    products: List[Dict[str, Any]] = []
    orders: List[Dict[str, Any]] = []
    transactions: List[Dict[str, Any]] = []

    files = []
    if os.path.exists(STAGING_DIR):
        files = [os.path.join(STAGING_DIR, p) for p in os.listdir(STAGING_DIR) if p.endswith((".json", ".csv"))]

    def _parse_shopify(payload) -> List[Dict[str, Any]]:
        out_orders: List[Dict[str, Any]] = []
        items = payload if isinstance(payload, list) else payload.get("orders") or payload.get("data") or []
        for it in items:
            total = it.get("total_price") or it.get("total") or (it.get("current_total_price") and it.get("current_total_price").get("amount"))
            subtotal = it.get("subtotal_price") or it.get("subtotal")
            tax = it.get("total_tax") or it.get("tax")
            ship = None
            if isinstance(it.get("shipping_lines"), list) and len(it.get("shipping_lines")) > 0:
                try:
                    ship = sum(float(s.get("price", 0) or 0) for s in it.get("shipping_lines"))
                except Exception:
                    ship = None
            ship = ship or it.get("shipping_total") or it.get("shipping")

            order_id = str(it.get("id") or it.get("order_number") or it.get("name") or it.get("number"))
            order_date = it.get("created_at") or it.get("date_created") or it.get("created")
            customer_obj = it.get("customer") or {}
            cust_id = customer_obj.get("id") or it.get("customer_id") or (it.get("email") and f"email:{it.get('email')}")

            order = {
                "order_id": order_id,
                "order_date": order_date,
                "customer_id": cust_id,
                "currency": it.get("currency"),
                "total_amount": total,
                "subtotal": subtotal,
                "tax_amount": tax,
                "shipping_amount": ship,
                "item_count": len(it.get("line_items") or []),
                "shipping_address": it.get("shipping_address") or {},
                "billing_address": it.get("billing_address") or {},
                "line_items": it.get("line_items") or [],
                "raw_payload": it,
            }
            out_orders.append(order)
        return out_orders

    def _parse_stripe(payload) -> List[Dict[str, Any]]:
        out_tx: List[Dict[str, Any]] = []
        items = payload if isinstance(payload, list) else payload.get("data") or payload.get("charges") or []
        for it in items:
            amt = it.get("amount")
            try:
                amount_val = float(amt) / 100.0 if amt is not None else None
            except Exception:
                amount_val = None

            created = it.get("created")
            if isinstance(created, (int, float)):
                try:
                    import datetime as _dt

                    created_iso = _dt.datetime.utcfromtimestamp(int(created)).isoformat()
                except Exception:
                    created_iso = None
            else:
                created_iso = it.get("created_at") or it.get("created")

            tx = {
                "transaction_id": it.get("id") or it.get("balance_transaction"),
                "transaction_date": created_iso,
                "order_id": it.get("metadata", {}).get("order_id") or it.get("invoice"),
                "customer_id": it.get("customer") or it.get("metadata", {}).get("customer_id"),
                "payment_provider": "stripe",
                "amount": amount_val,
                "currency": it.get("currency"),
                "status": it.get("status") or ("paid" if it.get("paid") else None),
                "raw_payload": it,
            }
            out_tx.append(tx)
        return out_tx

    for path in files:
        try:
            if path.endswith(".json"):
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            else:
                continue

            name = os.path.basename(path).lower()
            if "shopify" in name:
                parsed_orders = _parse_shopify(payload)
                orders.extend(parsed_orders)
                for it in parsed_orders:
                    customers.append(
                        {
                            "customer_id": it.get("customer_id") or f"guest_{it.get('order_id')}",
                            "email": (it.get("raw_payload", {}).get("email") or None),
                            "name": (it.get("raw_payload", {}).get("customer", {}).get("first_name") if isinstance(it.get("raw_payload", {}).get("customer"), dict) else None),
                            "created_at": it.get("order_date"),
                            "last_order_id": it.get("order_id"),
                            "total_lifetime_value": None,
                            "metadata": {},
                        }
                    )
            elif "stripe" in name:
                parsed_tx = _parse_stripe(payload)
                transactions.extend(parsed_tx)
            elif "woocommerce" in name or "orders" in name:
                items = payload if isinstance(payload, list) else payload.get("orders") or payload.get("data") or []
                for it in items:
                    order = {
                        "order_id": it.get("id") or it.get("order_id") or str(it.get("number")),
                        "order_date": it.get("date_created") or it.get("created_at"),
                        "customer_id": it.get("customer_id") or (it.get("billing") and it.get("billing").get("email")),
                        "currency": it.get("currency") or it.get("currency_code"),
                        "total_amount": it.get("total") or it.get("total_price"),
                        "subtotal": it.get("subtotal"),
                        "tax_amount": it.get("total_tax") or it.get("tax"),
                        "shipping_amount": it.get("shipping_total") or it.get("shipping"),
                        "item_count": len(it.get("line_items") or []),
                        "shipping_address": it.get("shipping") or {},
                        "billing_address": it.get("billing") or {},
                        "line_items": it.get("line_items") or [],
                        "raw_payload": it,
                    }
                    orders.append(order)
                    customers.append(
                        {
                            "customer_id": order.get("customer_id") or f"guest_{order.get('order_id')}",
                            "email": (it.get("billing") and it.get("billing").get("email")),
                            "name": (it.get("billing") and it.get("billing").get("name")),
                            "created_at": order.get("order_date"),
                            "last_order_id": order.get("order_id"),
                            "total_lifetime_value": None,
                            "metadata": {},
                        }
                    )
            elif "paypal" in name or "transactions" in name:
                items = payload if isinstance(payload, list) else payload.get("transactions") or payload.get("data") or []
                for it in items:
                    tx = {
                        "transaction_id": it.get("transaction_id") or it.get("id"),
                        "transaction_date": it.get("transaction_initiation_date") or it.get("transaction_updated_date") or it.get("create_time"),
                        "order_id": None,
                        "customer_id": it.get("payer") and it.get("payer").get("payer_id") or None,
                        "payment_provider": "paypal",
                        "amount": it.get("amount") or (it.get("gross_amount") and it.get("gross_amount").get("value")),
                        "currency": (it.get("amount") and it.get("amount").get("currency")) or it.get("currency"),
                        "status": it.get("status"),
                        "raw_payload": it,
                    }
                    transactions.append(tx)
        except Exception:
            logger.exception("Failed processing staged file %s", path)

    # Convert to DataFrame to run transforms
    if orders:
        df_orders = pd.DataFrame(orders)
        if "order_date" in df_orders.columns:
            df_orders = transform.normalize_datetime(df_orders, ["order_date"], tz="UTC")
        if "total_amount" in df_orders.columns:
            df_orders = transform.normalize_currency(df_orders, amount_col="total_amount", currency_col="currency", target_currency=os.environ.get("TARGET_CURRENCY", "USD"))
            df_orders["order_total"] = df_orders.get("total_amount_normalized")
        if "order_total" not in df_orders.columns:
            df_orders = transform.calculate_order_total(df_orders)
        df_orders = transform.flag_fraud(df_orders)
        df_orders = transform.validate_orders(df_orders)

        orders = df_orders.to_dict(orient="records")
        try:
            clv_df = transform.calculate_clv(df_orders)
            for c in customers:
                cid = c.get("customer_id")
                row = clv_df[clv_df["customer_id"] == cid]
                if not row.empty:
                    c["total_lifetime_value"] = float(row.iloc[0].get("clv", 0))
        except Exception:
            logger.exception("Error calculating CLV")

    if customers:
        loader.upsert_customers(customers)
    if products:
        loader.upsert_products(products)
    if orders:
        loader.upsert_orders(orders)
    if transactions:
        loader.upsert_transactions(transactions)


default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _notify_failure,
}


with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Simple ETL: ingest -> transform -> load",
    schedule=SCHEDULE,
    start_date=timezone.utcnow() - timedelta(days=1),
    catchup=False,
) as dag:

    t_ingest = PythonOperator(task_id="ingest", python_callable=run_ingest)

    t_transform = PythonOperator(task_id="transform", python_callable=run_transform)

    t_ingest >> t_transform

