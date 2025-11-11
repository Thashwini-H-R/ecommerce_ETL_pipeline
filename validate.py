"""Validation utilities for ETL pipeline.

Provides functions to validate lists of dicts (orders, customers, transactions).
Includes a small CLI to run quick checks on staged files.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple


def validate_orders_list(orders: List[Dict[str, Any]], required_fields: List[str] | None = None) -> List[Tuple[str, List[str]]]:
    """Validate a list of order dicts.

    Returns list of tuples (order_id_or_index, list_of_issues).
    """
    required_fields = required_fields or ["order_id", "customer_id", "order_date", "order_total"]
    results: List[Tuple[str, List[str]]] = []

    for idx, o in enumerate(orders):
        issues: List[str] = []
        oid = o.get("order_id") or f"idx:{idx}"
        for f in required_fields:
            if f not in o or o.get(f) is None or (isinstance(o.get(f), str) and o.get(f).strip() == ""):
                issues.append(f"missing_{f}")

        # basic numeric checks
        try:
            if "order_total" in o and o.get("order_total") is not None:
                val = float(o.get("order_total"))
                if val < 0:
                    issues.append("negative_order_total")
        except Exception:
            issues.append("invalid_order_total")

        results.append((str(oid), issues))

    return results


def validate_customers_list(customers: List[Dict[str, Any]]) -> List[Tuple[str, List[str]]]:
    results: List[Tuple[str, List[str]]] = []
    for idx, c in enumerate(customers):
        issues: List[str] = []
        cid = c.get("customer_id") or f"idx:{idx}"
        if not c.get("customer_id"):
            issues.append("missing_customer_id")
        email = c.get("email")
        if email and "@" not in email:
            issues.append("invalid_email")
        results.append((str(cid), issues))
    return results


def validate_transactions_list(transactions: List[Dict[str, Any]]) -> List[Tuple[str, List[str]]]:
    results: List[Tuple[str, List[str]]] = []
    for idx, t in enumerate(transactions):
        issues: List[str] = []
        tid = t.get("transaction_id") or f"idx:{idx}"
        if not t.get("transaction_id"):
            issues.append("missing_transaction_id")
        if not t.get("amount") and t.get("amount") != 0:
            issues.append("missing_amount")
        results.append((str(tid), issues))
    return results


def raise_if_issues(issues: List[Tuple[str, List[str]]], name: str = "items") -> None:
    bad = [i for i in issues if i[1]]
    if bad:
        lines = [f"{ident}: {', '.join(iss)}" for ident, iss in bad]
        raise ValueError(f"Validation failed for {name}:\n" + "\n".join(lines))


def cli_check_staging(staging_dir: str = "./staging") -> None:
    files = [os.path.join(staging_dir, p) for p in os.listdir(staging_dir) if p.endswith(".json")] if os.path.exists(staging_dir) else []
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        name = os.path.basename(path).lower()
        if any(k in name for k in ("order", "shopify", "woocommerce")):
            items = payload if isinstance(payload, list) else payload.get("orders") or payload.get("data") or []
            issues = validate_orders_list(items)
            raise_if_issues(issues, name)
        if any(k in name for k in ("stripe", "charge", "transactions", "paypal")):
            items = payload if isinstance(payload, list) else payload.get("data") or payload.get("transactions") or []
            issues = validate_transactions_list(items)
            raise_if_issues(issues, name)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--staging", default="./staging")
    args = p.parse_args()
    cli_check_staging(args.staging)
