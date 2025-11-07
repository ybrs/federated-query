-- Schema initialization for federated query testing
-- This creates sample tables for testing the query engine

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Public schema tables
CREATE TABLE IF NOT EXISTS public.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES public.customers(id),
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES public.orders(id),
    product_id INTEGER NOT NULL REFERENCES public.products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL
);

-- Staging schema tables (for testing multi-schema queries)
CREATE TABLE IF NOT EXISTS staging.temp_sales (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    quantity INTEGER,
    sale_date DATE,
    revenue DECIMAL(10, 2)
);

-- Create indexes for better query performance
CREATE INDEX idx_orders_customer_id ON public.orders(customer_id);
CREATE INDEX idx_orders_order_date ON public.orders(order_date);
CREATE INDEX idx_order_items_order_id ON public.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON public.order_items(product_id);
CREATE INDEX idx_customers_region ON public.customers(region);

-- Create a view for testing
CREATE OR REPLACE VIEW public.customer_order_summary AS
SELECT
    c.id,
    c.name,
    c.region,
    COUNT(o.id) as total_orders,
    COALESCE(SUM(o.amount), 0) as total_amount
FROM public.customers c
LEFT JOIN public.orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.region;
