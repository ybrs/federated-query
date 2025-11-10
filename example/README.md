# Federated Query Example

This example demonstrates the federated query engine with **multiple datasources** (PostgreSQL and DuckDB) to prove Phase 2 JOIN support works across different database systems.

## Overview

The example demonstrates:
- Loading configuration from a YAML file with **multiple datasources**
- Creating a catalog that spans PostgreSQL and DuckDB
- Creating and registering datasources
- Loading metadata from heterogeneous data sources
- Setting up a federated catalog for cross-database queries

## Datasources

This example uses TWO datasources:

1. **postgres_prod**: PostgreSQL database containing `orders` table
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
2. **Creates PostgreSQL connection** and populates `orders` table
3. **Creates in-memory DuckDB** and populates `customers` table
4. **Registers both datasources** with the catalog
5. **Loads metadata** from both databases into a unified catalog
6. **Displays federated catalog** showing tables from both systems

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
id  | customer_id | amount  | order_date
----|-------------|---------|------------
101 | 1           | 1500.00 | 2024-01-15
102 | 2           | 750.00  | 2024-01-16
103 | 1           | 2200.00 | 2024-01-17
```

**DuckDB (customers table):**
```
id | name          | email
---|---------------|-------------------
1  | Alice Smith   | alice@example.com
2  | Bob Jones     | bob@example.com
3  | Charlie Brown | charlie@example.com
```

## Federated Query Example

With both datasources registered in the catalog, you can execute cross-database queries:

```sql
SELECT o.id, c.name, o.amount
FROM postgres_prod.public.orders o
JOIN local_duckdb.main.customers c ON o.customer_id = c.id
WHERE o.amount > 1000
```

This query joins data from PostgreSQL (`orders`) with data from DuckDB (`customers`).

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
- Build complex multi-source queries
