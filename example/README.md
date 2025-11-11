# Federated Query Examples

This directory contains examples demonstrating the federated query engine with **multiple datasources** (PostgreSQL and DuckDB). These examples showcase Phase 2 (JOINs) and Phase 3 (Aggregations) capabilities across different database systems.

## Overview

The examples demonstrate:
- Loading configuration from a YAML file with **multiple datasources**
- Creating a catalog that spans PostgreSQL and DuckDB
- Creating and registering datasources
- Loading metadata from heterogeneous data sources
- Setting up a federated catalog for cross-database queries
- **Federated JOINs** across different databases
- **Aggregations with GROUP BY** (COUNT, SUM, AVG, MIN, MAX)
- **Combined JOINs and Aggregations** for complex analytics

## Available Examples

### 1. `query.py` - Full Featured Demo
Demonstrates federated queries with JOINs and aggregations across PostgreSQL and DuckDB.

### 2. `aggregate_queries.py` - Aggregation Showcase
Focused examples of Phase 3 aggregation features using DuckDB.

## Datasources

The main example (`query.py`) uses TWO datasources:

1. **postgres_prod**: PostgreSQL database containing `orders` table (with region data)
2. **local_duckdb**: In-memory DuckDB containing `customers` table

This setup demonstrates the ability to query across different database systems.

## Prerequisites

### Option 1: With PostgreSQL (Full Federated Example)

Start PostgreSQL using Docker:

```bash
docker-compose up -d postgres
```

Wait for PostgreSQL to be ready:

```bash
docker-compose ps
```

### Option 2: Without PostgreSQL (DuckDB Only)

The example will gracefully skip PostgreSQL if it's not available and run with DuckDB only. This is useful for quick testing but doesn't demonstrate the full federated capability.

## Running the Example

From the project root:

```bash
PYTHONPATH=/home/user/federated-query python example/query.py
```

Or after installing the package:

```bash
pip install -e .
python example/query.py
```

## What the Example Does

1. **Loads configuration** from `dbconfig.yaml` with 2 datasources
2. **Creates PostgreSQL connection** and populates `orders` table with sample data
3. **Creates in-memory DuckDB** and populates `customers` table
4. **Registers both datasources** with the catalog
5. **Loads metadata** from both databases into a unified catalog
6. **Displays federated catalog** showing tables from both systems
7. **Executes 5 example queries** demonstrating various capabilities:
   - Federated JOIN across databases
   - JOIN with WHERE clause filtering
   - Aggregations with GROUP BY
   - Combined JOIN + aggregation
   - Global aggregation (without GROUP BY)

## Sample Output (With Both Datasources)

```
Loaded config with 2 datasource(s)

Setting up datasource: postgres_prod (postgresql)
  Created orders table in PostgreSQL
  Successfully registered postgres_prod

Setting up datasource: local_duckdb (duckdb)
  Created customers table in DuckDB
  Successfully registered local_duckdb

Loading metadata from datasources...

Catalog summary: Catalog(datasources=2, schemas=4)

Schemas and tables in catalog:

  postgres_prod.public:
    orders:
      - id: INTEGER NOT NULL
      - customer_id: INTEGER NULL
      - amount: DOUBLE NULL
      - order_date: DATE NULL

  local_duckdb.main:
    customers:
      - id: INTEGER NULL
      - name: VARCHAR NULL
      - email: VARCHAR NULL

============================================================
Federated catalog setup complete!
============================================================
Total datasources: 2
Total schemas: 4
Total tables: 2

You can now execute federated queries across:
  - postgres_prod
  - local_duckdb
```

## Sample Data

**PostgreSQL (orders table):**
```
id  | customer_id | amount  | region | order_date
----|-------------|---------|--------|------------
101 | 1           | 1500.00 | North  | 2024-01-15
102 | 2           | 750.00  | South  | 2024-01-16
103 | 1           | 2200.00 | North  | 2024-01-17
104 | 3           | 1200.00 | East   | 2024-01-18
105 | 2           | 900.00  | South  | 2024-01-19
106 | 1           | 1800.00 | North  | 2024-01-20
```

**DuckDB (customers table):**
```
id | name          | email
---|---------------|-------------------
1  | Alice Smith   | alice@example.com
2  | Bob Jones     | bob@example.com
3  | Charlie Brown | charlie@example.com
```

## Example Queries

The `query.py` script demonstrates 5 different types of queries:

### 1. Federated JOIN - Customers with Orders
```sql
SELECT c.name, c.id, o.amount, o.region
FROM local_duckdb.main.customers c
JOIN postgres_prod.public.orders o ON c.id = o.customer_id
```
Joins data from DuckDB (`customers`) with PostgreSQL (`orders`).

### 2. JOIN with WHERE Clause - High-Value Orders
```sql
SELECT c.name, o.amount
FROM local_duckdb.main.customers c
JOIN postgres_prod.public.orders o ON c.id = o.customer_id
WHERE o.amount > 1000
```
Demonstrates filtering on federated JOIN results.

### 3. Aggregation - Sales by Region
```sql
SELECT
    region,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM postgres_prod.public.orders
GROUP BY region
```
Shows GROUP BY with multiple aggregate functions (COUNT, SUM, AVG).

### 4. Federated JOIN + Aggregation - Customer Spending
```sql
SELECT
    c.name,
    COUNT(*) as order_count,
    SUM(o.amount) as total_spent
FROM local_duckdb.main.customers c
JOIN postgres_prod.public.orders o ON c.id = o.customer_id
GROUP BY c.name
```
Combines federated JOIN with aggregation for customer analytics.

### 5. Global Aggregation - Overall Statistics
```sql
SELECT
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as average_order,
    MIN(amount) as smallest_order,
    MAX(amount) as largest_order
FROM postgres_prod.public.orders
```
Demonstrates aggregation without GROUP BY (global aggregates).

## Configuration

The `dbconfig.yaml` file configures:
- **Datasources**: PostgreSQL and DuckDB connections
- **Optimizer settings**: Query optimization options (predicate pushdown, join reordering)
- **Executor settings**: Query execution parameters (memory, batch size)

## Next Steps

After understanding this federated setup, you can:
- Execute federated queries across PostgreSQL and DuckDB
- Add more datasources to the configuration
- Experiment with different optimizer settings
- Test JOIN performance across heterogeneous sources
- Build complex multi-source queries with JOINs and aggregations
- Explore advanced aggregations (see `aggregate_queries.py`)
- Combine multiple data sources for analytics

## Supported Features

### Phase 2: JOINs ✅
- INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
- Cross-database JOINs (federated)
- Multi-table JOINs
- JOIN with WHERE clause filtering

### Phase 3: Aggregations ✅
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- GROUP BY with single or multiple columns
- Global aggregations (without GROUP BY)
- Multiple aggregates in single query
- Aggregations on federated JOIN results

### Future Phases
- HAVING clause evaluation (parsed but not yet evaluated)
- Subqueries and CTEs
- Window functions
- Advanced optimizations
