-- Warehouse schema for ecommerce ETL
-- Dimensions: customers_dim, products_dim
-- Facts: orders_fact, transactions_fact
-- Uses TimescaleDB to create hypertables on time columns for efficient time-series analytics

-- Customers dimension
CREATE TABLE IF NOT EXISTS customers_dim (
    customer_id TEXT PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMPTZ,
    last_order_id TEXT,
    total_lifetime_value NUMERIC,
    metadata JSONB
);

-- Products dimension
CREATE TABLE IF NOT EXISTS products_dim (
    product_id TEXT PRIMARY KEY,
    sku TEXT,
    name TEXT,
    category TEXT,
    list_price NUMERIC,
    active BOOLEAN DEFAULT TRUE,
    metadata JSONB
);

-- Orders fact table (time-series)
CREATE TABLE IF NOT EXISTS orders_fact (
    order_id TEXT PRIMARY KEY,
    order_date TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL,
    currency TEXT,
    total_amount NUMERIC,
    subtotal NUMERIC,
    tax_amount NUMERIC,
    shipping_amount NUMERIC,
    item_count INTEGER,
    shipping_address JSONB,
    billing_address JSONB,
    line_items JSONB,
    raw_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT fk_orders_customer FOREIGN KEY(customer_id) REFERENCES customers_dim(customer_id)
);

-- Transactions fact table (time-series)
CREATE TABLE IF NOT EXISTS transactions_fact (
    transaction_id TEXT PRIMARY KEY,
    transaction_date TIMESTAMPTZ NOT NULL,
    order_id TEXT,
    customer_id TEXT,
    payment_provider TEXT,
    amount NUMERIC,
    currency TEXT,
    status TEXT,
    raw_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT fk_transactions_order FOREIGN KEY(order_id) REFERENCES orders_fact(order_id),
    CONSTRAINT fk_transactions_customer FOREIGN KEY(customer_id) REFERENCES customers_dim(customer_id)
);

-- Create hypertables for time-series facts (requires TimescaleDB)
-- Note: run as a superuser / db owner that can call create_hypertable
SELECT create_extension('timescaledb') WHERE NOT EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
);

-- Wrap in DO block to avoid errors when hypertable already exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'orders_fact') THEN
        PERFORM create_hypertable('orders_fact', 'order_date', migrate_data => TRUE);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'transactions_fact') THEN
        PERFORM create_hypertable('transactions_fact', 'transaction_date', migrate_data => TRUE);
    END IF;
END$$;

-- Indexes to speed up common queries
CREATE INDEX IF NOT EXISTS idx_orders_customer_date ON orders_fact (customer_id, order_date DESC);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders_fact (order_date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions_fact (transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_products_sku ON products_dim (sku);
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers_dim (email);

-- Upsert helper: we rely on INSERT ... ON CONFLICT in loaders
