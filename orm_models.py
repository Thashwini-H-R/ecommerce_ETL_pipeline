"""SQLAlchemy ORM models for the warehouse schema.

These mirror `sql/warehouse_schema.sql`. This file is optional — the loader uses psycopg2 by default.
Requires SQLAlchemy to use; the file is provided as a helpful reference and for optional ORM-based workflows.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers_dim"

    customer_id = Column(String, primary_key=True)
    email = Column(String, index=True)
    name = Column(String)
    created_at = Column(DateTime)
    last_order_id = Column(String)
    total_lifetime_value = Column(Numeric)
    metadata = Column(JSON)

    orders = relationship("OrderFact", back_populates="customer")


class Product(Base):
    __tablename__ = "products_dim"

    product_id = Column(String, primary_key=True)
    sku = Column(String, index=True)
    name = Column(String)
    category = Column(String)
    list_price = Column(Numeric)
    active = Column(Boolean, default=True)
    metadata = Column(JSON)


class OrderFact(Base):
    __tablename__ = "orders_fact"

    order_id = Column(String, primary_key=True)
    order_date = Column(DateTime, index=True)
    customer_id = Column(String, ForeignKey("customers_dim.customer_id"))
    currency = Column(String)
    total_amount = Column(Numeric)
    subtotal = Column(Numeric)
    tax_amount = Column(Numeric)
    shipping_amount = Column(Numeric)
    item_count = Column(Integer)
    shipping_address = Column(JSON)
    billing_address = Column(JSON)
    line_items = Column(JSON)
    raw_payload = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    customer = relationship("Customer", back_populates="orders")


class TransactionFact(Base):
    __tablename__ = "transactions_fact"

    transaction_id = Column(String, primary_key=True)
    transaction_date = Column(DateTime, index=True)
    order_id = Column(String, ForeignKey("orders_fact.order_id"))
    customer_id = Column(String, ForeignKey("customers_dim.customer_id"))
    payment_provider = Column(String)
    amount = Column(Numeric)
    currency = Column(String)
    status = Column(String)
    raw_payload = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # relationships omitted for brevity
