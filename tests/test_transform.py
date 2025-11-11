import pandas as pd
import pytest

import transform


def test_remove_duplicates():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    cleaned, removed = transform.remove_duplicates(df)
    assert removed == 1
    assert len(cleaned) == 2


def test_impute_missing_numeric_and_categorical():
    df = pd.DataFrame({"num": [1, None, 3], "cat": ["a", None, "a"]})
    out = transform.impute_missing(df, numeric_strategy="median", categorical_strategy="mode")
    assert out.loc[1, "num"] == 2 or pytest.approx(out.loc[1, "num"])  # median is 2
    assert out.loc[1, "cat"] == "a"


def test_normalize_currency_with_rate_map():
    df = pd.DataFrame({"amount": ["$10.00", "€20.00"], "currency": ["USD", "EUR"]})
    rate_map = {"USD": 1.0, "EUR": 1.1}
    out = transform.normalize_currency(df, amount_col="amount", currency_col="currency", rate_map=rate_map, target_currency="USD")
    assert "amount_normalized" in out.columns or "amount_normalized" in out.columns
    # second row 20 EUR * 1.1 = 22
    assert pytest.approx(out.loc[1, "amount_normalized"], rel=1e-3) == 22.0


def test_calculate_order_total_basic():
    df = pd.DataFrame({"price": [10, 5], "quantity": [2, 3]})
    out = transform.calculate_order_total(df, price_col="price", qty_col="quantity", out_col="order_total")
    assert list(out["order_total"]) == [20, 15]


def test_calculate_clv():
    df = pd.DataFrame({"customer_id": ["c1", "c1", "c2"], "order_total": [10, 20, 5], "order_date": ["2020-01-01", "2020-02-01", "2020-03-01"]})
    out = transform.calculate_clv(df)
    row = out[out["customer_id"] == "c1"].iloc[0]
    assert row["clv"] == 30
    assert row["total_orders"] == 2


def test_flag_fraud():
    df = pd.DataFrame({"order_total": [2000, 50], "shipping_country": ["US", "US"], "billing_country": ["CN", "US"], "email": ["user@mailinator.com", "ok@example.com"], "billing_name": ["n", None], "billing_address": ["a", None]})
    out = transform.flag_fraud(df, high_value_threshold=1000)
    assert bool(out.loc[0, "fraud_flag"]) is True
    assert bool(out.loc[1, "fraud_flag"]) is False
