"""Data cleaning and transformation utilities for the e-commerce ETL pipeline.

Provides functions to:
- remove duplicates
- impute missing values
- normalize datetimes and currency amounts
- merge orders with product and customer metadata
- calculate derived metrics (order total, CLV)
- apply simple fraud flagging and business validations

These functions are intentionally small and composable so they can be used
inside Airflow tasks or local scripts.
"""

from __future__ import annotations

import logging
import math
import re
import requests
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger("transform")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def remove_duplicates(df: pd.DataFrame, subset: Optional[Iterable[str]] = None, keep: str = "first") -> Tuple[pd.DataFrame, int]:
    """Remove duplicate rows from DataFrame.

    Args:
        df: input DataFrame
        subset: columns to consider for identifying duplicates (None = all columns)
        keep: which duplicate to keep ('first', 'last', False)

    Returns:
        (cleaned_df, num_removed)
    """
    before = len(df)
    cleaned = df.drop_duplicates(subset=list(subset) if subset is not None else None, keep=keep)
    removed = before - len(cleaned)
    logger.info("Removed %d duplicate rows (from %d to %d)", removed, before, len(cleaned))
    return cleaned.reset_index(drop=True), removed


def impute_missing(
    df: pd.DataFrame,
    numeric_strategy: str = "median",
    categorical_strategy: str = "mode",
    datetime_strategy: str = "ffill",
    fill_values: Optional[Dict[str, object]] = None,
) -> pd.DataFrame:
    """Impute missing values for numeric, categorical and datetime columns.

    - numeric_strategy: 'median'|'mean'|'zero'|'ffill'|'bfill'
    - categorical_strategy: 'mode'|'unknown'|'ffill'|'bfill'
    - datetime_strategy: 'ffill'|'bfill'|'epoch' (set to 1970-01-01)
    - fill_values: explicit mapping column -> value (overrides strategies)
    """
    df = df.copy()
    fill_values = fill_values or {}

    for col in df.columns:
        if col in fill_values:
            df[col] = df[col].fillna(fill_values[col])
            continue

        if pd.api.types.is_numeric_dtype(df[col]):
            if numeric_strategy == "median":
                val = df[col].median()
            elif numeric_strategy == "mean":
                val = df[col].mean()
            elif numeric_strategy == "zero":
                val = 0
            elif numeric_strategy in ("ffill", "bfill"):
                df[col] = df[col].fillna(method=numeric_strategy)
                continue
            else:
                val = 0
            if pd.isna(val):
                val = 0
            df[col] = df[col].fillna(val)

        elif pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]) and _looks_like_datetime_series(df[col]):
            # try to parse
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass

            if datetime_strategy in ("ffill", "bfill"):
                df[col] = df[col].fillna(method=datetime_strategy)
            elif datetime_strategy == "epoch":
                df[col] = df[col].fillna(pd.to_datetime("1970-01-01"))
            else:
                df[col] = df[col]

        else:
            # categorical / object
            if categorical_strategy == "mode":
                try:
                    mode_val = df[col].mode(dropna=True)
                    val = mode_val.iloc[0] if len(mode_val) > 0 else ""
                except Exception:
                    val = ""
                df[col] = df[col].fillna(val)
            elif categorical_strategy == "unknown":
                df[col] = df[col].fillna("<unknown>")
            elif categorical_strategy in ("ffill", "bfill"):
                df[col] = df[col].fillna(method=categorical_strategy)
            else:
                df[col] = df[col].fillna("")

    return df


def _looks_like_datetime_series(s: pd.Series) -> bool:
    # Heuristic: many string values contain '-' or ':' or 'T'
    sample = s.dropna().astype(str).head(20)
    if sample.empty:
        return False
    n_matches = sum(1 for v in sample if any(c in v for c in ["-", ":", "T"]))
    return n_matches >= max(1, len(sample) // 4)


def normalize_datetime(df: pd.DataFrame, columns: Iterable[str], tz: Optional[str] = None, fmt: Optional[str] = None) -> pd.DataFrame:
    """Normalize datetime columns to pandas datetime and optionally convert tz/format.

    Args:
        df: input DataFrame
        columns: iterable of column names to normalize
        tz: timezone to convert to, e.g. 'UTC'
        fmt: if provided, format the column back to string using this format
    """
    df = df.copy()
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        if tz:
            try:
                df[col] = df[col].dt.tz_localize("UTC").dt.tz_convert(tz)
            except Exception:
                # if already tz-aware, just convert
                try:
                    df[col] = df[col].dt.tz_convert(tz)
                except Exception:
                    pass
        if fmt:
            df[col] = df[col].dt.strftime(fmt)
    return df


_CURRENCY_RE = re.compile(r"[^0-9\.-]")


def normalize_currency(df: pd.DataFrame, amount_col: str, currency_col: Optional[str] = None, rate_map: Optional[Dict[str, float]] = None, target_currency: str = "USD") -> pd.DataFrame:
    """Normalize currency amounts into a target currency (float).

    - amount_col: column that contains numeric or string amounts
    - currency_col: optional column with currency code (e.g. 'USD', 'EUR')
    - rate_map: mapping from currency code to conversion rate to target_currency (1 means same)
    """
    df = df.copy()
    rate_map = rate_map or {}

    # If no rate_map provided and currency_col exists, attempt to fetch live FX rates
    if not rate_map and currency_col and currency_col in df.columns:
        try:
            # fetch rates relative to target_currency
            resp = requests.get(f"https://api.exchangerate.host/latest?base={target_currency}")
            if resp.status_code == 200:
                data = resp.json()
                rates = data.get("rates", {})
                # convert to mapping currency -> rate_to_target (i.e., multiply by 1/rate)
                # since rates are target_currency -> other, we invert
                rate_map = {cur: 1.0 / float(r) if r else 1.0 for cur, r in rates.items()}
                # include target currency with rate 1
                rate_map[target_currency] = 1.0
                logger.info("Fetched %d FX rates for currency normalization", len(rate_map))
        except Exception:
            logger.exception("Failed to fetch FX rates; proceeding without live conversion")

    def _parse_amount(v):
        if pd.isna(v):
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        # strip currency symbols
        s = _CURRENCY_RE.sub("", s)
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None

    parsed = df[amount_col].map(_parse_amount)

    if currency_col and currency_col in df.columns:
        def _to_target(row):
            amt = row[amount_col]
            cur = row[currency_col]
            a = _parse_amount(amt)
            if a is None:
                return None
            rate = rate_map.get(cur, 1.0)
            return a * rate

        df[amount_col + "_normalized"] = df.apply(_to_target, axis=1)
    else:
        # assume amounts already in target
        df[amount_col + "_normalized"] = parsed

    return df


def merge_metadata(df_orders: pd.DataFrame, df_products: Optional[pd.DataFrame] = None, df_customers: Optional[pd.DataFrame] = None, on_product: str = "product_id", on_customer: str = "customer_id") -> pd.DataFrame:
    """Merge orders with product and customer metadata (left joins).

    Returns a new DataFrame with product_ and customer_ prefixes to avoid collisions.
    """
    df = df_orders.copy()
    if df_products is not None and on_product in df.columns:
        prod = df_products.copy()
        prod = prod.add_prefix("product_")
        # rename key back so join works
        prod = prod.rename(columns={f"product_{on_product}": on_product})
        df = df.merge(prod, on=on_product, how="left")

    if df_customers is not None and on_customer in df.columns:
        cust = df_customers.copy()
        cust = cust.add_prefix("customer_")
        cust = cust.rename(columns={f"customer_{on_customer}": on_customer})
        df = df.merge(cust, on=on_customer, how="left")

    return df


def calculate_order_total(df: pd.DataFrame, price_col: str = "price", qty_col: str = "quantity", discount_col: Optional[str] = None, tax_col: Optional[str] = None, shipping_col: Optional[str] = None, out_col: str = "order_total") -> pd.DataFrame:
    """Calculate order total using price * qty +/- adjustments.

    Supports optional discount (absolute) or tax/shipping additions.
    """
    df = df.copy()
    price = pd.to_numeric(df.get(price_col, 0), errors="coerce").fillna(0)
    qty = pd.to_numeric(df.get(qty_col, 1), errors="coerce").fillna(1)
    total = price * qty
    if discount_col and discount_col in df.columns:
        disc = pd.to_numeric(df[discount_col], errors="coerce").fillna(0)
        total = total - disc
    if tax_col and tax_col in df.columns:
        tax = pd.to_numeric(df[tax_col], errors="coerce").fillna(0)
        total = total + tax
    if shipping_col and shipping_col in df.columns:
        ship = pd.to_numeric(df[shipping_col], errors="coerce").fillna(0)
        total = total + ship

    df[out_col] = total
    return df


def calculate_clv(df_orders: pd.DataFrame, customer_id_col: str = "customer_id", order_total_col: str = "order_total") -> pd.DataFrame:
    """Calculate customer lifetime value (simple sum-based CLV) and RFM indicators.

    Returns a DataFrame keyed by customer_id with columns: clv, total_orders, avg_order_value, recency_days, frequency
    """
    df = df_orders.copy()
    df[order_total_col] = pd.to_numeric(df.get(order_total_col, 0), errors="coerce").fillna(0)
    df["order_date"] = pd.to_datetime(df.get("order_date", None), errors="coerce")
    now = pd.Timestamp.utcnow()
    # ensure compatible tz-awareness between now and last_order_date
    try:
        # if now is tz-aware and last_order_date is naive, make now naive
        if getattr(now, "tzinfo", None) is not None:
            now = now.tz_convert(None)
    except Exception:
        try:
            now = pd.Timestamp(now).tz_localize(None)
        except Exception:
            pass
    agg = df.groupby(customer_id_col).agg(
        clv=(order_total_col, "sum"),
        total_orders=(order_total_col, "count"),
        avg_order_value=(order_total_col, "mean"),
        last_order_date=("order_date", "max"),
    )
    agg = agg.reset_index()
    agg["recency_days"] = (now - agg["last_order_date"]).dt.days
    agg["frequency"] = agg["total_orders"]
    agg["avg_order_value"] = agg["avg_order_value"].fillna(0)
    agg["clv"] = agg["clv"].fillna(0)
    return agg


DISPOSABLE_EMAIL_DOMAINS = {"mailinator.com", "10minutemail.com", "tempmail.com", "trashmail.com"}


def flag_fraud(df: pd.DataFrame, high_value_threshold: float = 1000.0, suspicious_email_domains: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Apply simple fraud rules and return DataFrame with fraud_score and fraud_flag columns.

    Rules (simple examples):
    - high value order (order_total > threshold)
    - billing vs shipping country mismatch
    - disposable email domain
    - missing required identity fields
    """
    df = df.copy()
    suspicious_email_domains = set(suspicious_email_domains or []) | DISPOSABLE_EMAIL_DOMAINS

    def _email_domain(email: Optional[str]) -> Optional[str]:
        if not email or not isinstance(email, str):
            return None
        parts = email.split("@")
        return parts[-1].lower() if len(parts) == 2 else None

    scores = []
    flags = []
    for _, row in df.iterrows():
        score = 0
        # high value
        ot = row.get("order_total")
        try:
            otv = float(ot) if ot is not None and not (isinstance(ot, float) and math.isnan(ot)) else 0.0
        except Exception:
            otv = 0.0
        if otv >= high_value_threshold:
            score += 3

        # country mismatch
        if row.get("shipping_country") and row.get("billing_country") and row.get("shipping_country") != row.get("billing_country"):
            score += 2

        # disposable email
        ed = _email_domain(row.get("email"))
        if ed and ed in suspicious_email_domains:
            score += 3

        # missing identity
        missing_identity = any(not row.get(c) for c in ("billing_name", "billing_address"))
        if missing_identity:
            score += 1

        flags.append(score >= 4)
        scores.append(score)

    df["fraud_score"] = scores
    df["fraud_flag"] = flags
    return df


def validate_orders(df: pd.DataFrame, required_fields: Optional[Iterable[str]] = None, allowed_statuses: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Run business validation checks and return DataFrame with an issues column listing problems per row.

    - required_fields: list of columns that must be present and non-null
    - allowed_statuses: allowed values for the `status` column if present
    """
    df = df.copy()
    issues_list: List[List[str]] = []
    required_fields = list(required_fields or ["order_id", "customer_id", "order_date", "order_total"])
    allowed_statuses = set(allowed_statuses or {"paid", "pending", "refunded", "cancelled"})

    email_re = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")

    for _, row in df.iterrows():
        issues: List[str] = []
        for f in required_fields:
            if not row.get(f) and row.get(f) != 0:
                issues.append(f"missing_{f}")

        # status
        if "status" in row and row.get("status") and row.get("status") not in allowed_statuses:
            issues.append(f"invalid_status:{row.get('status')}")

        # email format
        e = row.get("email")
        if e and not email_re.match(str(e)):
            issues.append("invalid_email")

        issues_list.append(issues)

    df["validation_issues"] = issues_list
    return df


__all__ = [
    "remove_duplicates",
    "impute_missing",
    "normalize_datetime",
    "normalize_currency",
    "merge_metadata",
    "calculate_order_total",
    "calculate_clv",
    "flag_fraud",
    "validate_orders",
]
