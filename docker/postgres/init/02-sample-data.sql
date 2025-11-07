-- Sample data for testing federated queries
-- This data will be used to test query execution, joins, aggregations, etc.

-- Insert customers
INSERT INTO public.customers (name, email, region) VALUES
    ('Alice Johnson', 'alice@example.com', 'US'),
    ('Bob Smith', 'bob@example.com', 'US'),
    ('Carol White', 'carol@example.com', 'EU'),
    ('David Brown', 'david@example.com', 'EU'),
    ('Eve Davis', 'eve@example.com', 'APAC'),
    ('Frank Wilson', 'frank@example.com', 'APAC'),
    ('Grace Lee', 'grace@example.com', 'US'),
    ('Henry Taylor', 'henry@example.com', 'EU'),
    ('Ivy Martinez', 'ivy@example.com', 'US'),
    ('Jack Anderson', 'jack@example.com', 'APAC');

-- Insert products
INSERT INTO public.products (name, category, price, stock_quantity) VALUES
    ('Laptop Pro', 'Electronics', 1299.99, 50),
    ('Wireless Mouse', 'Electronics', 29.99, 200),
    ('Mechanical Keyboard', 'Electronics', 89.99, 100),
    ('Office Chair', 'Furniture', 249.99, 30),
    ('Standing Desk', 'Furniture', 499.99, 20),
    ('Monitor 27"', 'Electronics', 349.99, 75),
    ('USB-C Hub', 'Electronics', 49.99, 150),
    ('Desk Lamp', 'Furniture', 39.99, 80),
    ('Notebook Set', 'Stationery', 12.99, 300),
    ('Pen Pack', 'Stationery', 5.99, 500);

-- Insert orders
INSERT INTO public.orders (customer_id, amount, status, order_date) VALUES
    (1, 1329.98, 'completed', '2024-01-15'),
    (1, 89.99, 'completed', '2024-02-20'),
    (2, 499.99, 'completed', '2024-01-22'),
    (2, 349.99, 'shipped', '2024-03-10'),
    (3, 1599.97, 'completed', '2024-01-18'),
    (3, 12.99, 'completed', '2024-02-14'),
    (4, 249.99, 'completed', '2024-02-05'),
    (5, 89.99, 'cancelled', '2024-01-30'),
    (5, 1299.99, 'completed', '2024-03-01'),
    (6, 399.98, 'completed', '2024-02-28'),
    (7, 549.98, 'shipped', '2024-03-15'),
    (8, 29.99, 'completed', '2024-01-25'),
    (9, 749.98, 'completed', '2024-02-18'),
    (10, 1849.96, 'completed', '2024-03-05');

-- Insert order items
INSERT INTO public.order_items (order_id, product_id, quantity, unit_price) VALUES
    -- Order 1 (Alice)
    (1, 1, 1, 1299.99),
    (1, 2, 1, 29.99),
    -- Order 2 (Alice)
    (2, 3, 1, 89.99),
    -- Order 3 (Bob)
    (3, 5, 1, 499.99),
    -- Order 4 (Bob)
    (4, 6, 1, 349.99),
    -- Order 5 (Carol)
    (5, 1, 1, 1299.99),
    (5, 6, 1, 349.99),
    (5, 2, 1, 29.99),
    -- Order 6 (Carol)
    (6, 9, 1, 12.99),
    -- Order 7 (David)
    (7, 4, 1, 249.99),
    -- Order 8 (Eve - cancelled)
    (8, 3, 1, 89.99),
    -- Order 9 (Eve)
    (9, 1, 1, 1299.99),
    -- Order 10 (Frank)
    (10, 6, 1, 349.99),
    (10, 7, 1, 49.99),
    -- Order 11 (Grace)
    (11, 5, 1, 499.99),
    (11, 7, 1, 49.99),
    -- Order 12 (Henry)
    (12, 2, 1, 29.99),
    -- Order 13 (Ivy)
    (13, 1, 1, 1299.99),
    (14, 6, 2, 349.99),
    (14, 3, 1, 89.99),
    (14, 2, 1, 29.99);

-- Insert staging data
INSERT INTO staging.temp_sales (product_id, quantity, sale_date, revenue) VALUES
    (1, 5, '2024-03-01', 6499.95),
    (2, 20, '2024-03-01', 599.80),
    (3, 10, '2024-03-01', 899.90),
    (4, 3, '2024-03-02', 749.97),
    (5, 2, '2024-03-02', 999.98),
    (6, 8, '2024-03-03', 2799.92);

-- Analyze tables for statistics
ANALYZE public.customers;
ANALYZE public.orders;
ANALYZE public.products;
ANALYZE public.order_items;
ANALYZE staging.temp_sales;
