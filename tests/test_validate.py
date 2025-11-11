import pytest

import validate


def test_validate_orders_list_ok():
    orders = [{"order_id": "o1", "customer_id": "c1", "order_date": "2020-01-01", "order_total": 10}]
    issues = validate.validate_orders_list(orders)
    assert all(len(i[1]) == 0 for i in issues)


def test_validate_orders_list_missing():
    orders = [{"customer_id": "c1", "order_date": "2020-01-01"}]
    issues = validate.validate_orders_list(orders)
    assert any("missing_order_id" in iss for _, iss in issues)
