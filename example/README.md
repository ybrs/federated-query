# Federated Query Example

This example demonstrates how to use the federated query engine with configuration and catalog setup.

## Overview

The example shows:
- Loading configuration from a YAML file
- Creating a catalog
- Creating and registering datasources
- Loading metadata from datasources
- Inspecting the catalog structure

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

1. **Loads configuration** from `dbconfig.yaml`
2. **Creates an in-memory DuckDB** database with sample data
3. **Registers the datasource** with the catalog
4. **Loads metadata** (schemas, tables, columns) into the catalog
5. **Displays catalog contents** showing all discovered tables and columns

## Sample Output

```
Loaded config with 1 datasource(s)
Creating datasource: local_duckdb
Creating sample data in local_duckdb

Loading metadata from datasources...

Catalog summary: Catalog(datasources=1, schemas=3)

Schemas in catalog:
  local_duckdb.main:
    - customers (3 columns)
      - id: INTEGER
      - name: VARCHAR
      - email: VARCHAR
    - orders (4 columns)
      - id: INTEGER
      - customer_id: INTEGER
      - amount: DOUBLE
      - order_date: DATE
  ...

Catalog setup complete!
```

## Configuration

The `dbconfig.yaml` file configures:
- **Datasources**: Which databases to connect to
- **Optimizer settings**: Query optimization options
- **Executor settings**: Query execution parameters

## Next Steps

After understanding this basic setup, you can:
- Add more datasources to the configuration
- Execute queries across datasources
- Experiment with different optimizer settings
- Build your own federated queries
