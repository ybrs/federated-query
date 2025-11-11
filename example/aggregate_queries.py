"""Example aggregation queries demonstrating Phase 3 functionality with federated sources."""

from pathlib import Path

from federated_query.catalog.catalog import Catalog
from federated_query.config.config import ExecutorConfig, load_config
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.executor.executor import Executor
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser


def create_datasource(name, ds_config):
    """Instantiate a datasource from config entry."""
    if ds_config.type == "duckdb":
        return DuckDBDataSource(name, ds_config.config)
    if ds_config.type == "postgresql":
        return PostgreSQLDataSource(name, ds_config.config)
    raise ValueError(f"Unsupported datasource type: {ds_config.type}")


def create_duckdb_sample_data(datasource):
    """Create sample DuckDB tables used by the aggregation examples."""
    conn = datasource.connection

    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute(
        """
        CREATE TABLE products (
            product_id INTEGER,
            product_name VARCHAR,
            category VARCHAR,
            unit_price DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO products VALUES
        (1, 'Widget Pro', 'Electronics', 25.99),
        (2, 'Gadget Plus', 'Electronics', 45.50),
        (3, 'Tool Kit', 'Hardware', 89.99),
        (4, 'Smart Device', 'Electronics', 199.99),
        (5, 'Premium Tool', 'Hardware', 149.99)
        """
    )


def create_postgres_sample_data(datasource):
    """Create sample PostgreSQL tables used by the aggregation examples."""
    conn = datasource._get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
            cursor.execute(
                """
                CREATE TABLE orders (
                    order_id INTEGER PRIMARY KEY,
                    product_id INTEGER,
                    region VARCHAR(50),
                    quantity INTEGER,
                    order_date DATE
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO orders VALUES
                (101, 1, 'North', 5, '2024-01-15'),
                (102, 2, 'South', 3, '2024-01-16'),
                (103, 1, 'North', 8, '2024-01-17'),
                (104, 3, 'East', 2, '2024-01-18'),
                (105, 2, 'South', 6, '2024-01-19'),
                (106, 4, 'North', 1, '2024-01-20'),
                (107, 1, 'East', 4, '2024-01-21'),
                (108, 5, 'West', 2, '2024-01-22'),
                (109, 3, 'West', 3, '2024-01-23'),
                (110, 2, 'North', 7, '2024-01-24')
                """
            )
            conn.commit()
            print("  Created orders table in PostgreSQL")
    finally:
        datasource._return_connection(conn)


def setup_federated_data():
    """Set up datasources defined in dbconfig.yaml and seed sample data."""
    config_path = Path(__file__).parent / "dbconfig.yaml"
    config = load_config(str(config_path))

    print(f"Loaded config with {len(config.datasources)} datasource(s)\n")

    catalog = Catalog()
    duckdb_ds = None
    postgres_ds = None

    for ds_name, ds_config in config.datasources.items():
        print(f"Setting up datasource: {ds_name} ({ds_config.type})")
        datasource = None

        try:
            datasource = create_datasource(ds_name, ds_config)
            datasource.connect()

            if ds_config.type == "duckdb":
                create_duckdb_sample_data(datasource)
                duckdb_ds = datasource
                print("  Seeded products table in DuckDB")
            elif ds_config.type == "postgresql":
                create_postgres_sample_data(datasource)
                postgres_ds = datasource

            catalog.register_datasource(datasource)
            print(f"  Successfully registered {ds_name}")

        except Exception as e:
            print(f"  WARNING: Could not set up {ds_name}: {e}")
            if datasource is not None:
                try:
                    datasource.disconnect()
                except Exception:
                    pass
            print("  Continuing without this datasource\n")
            continue

        print()

    if duckdb_ds is None:
        raise RuntimeError(
            "DuckDB datasource is required for aggregation examples. "
            "Please ensure dbconfig.yaml defines an accessible DuckDB datasource."
        )

    if not catalog.datasources:
        raise RuntimeError("No datasources were successfully registered.")

    print("Loading metadata from datasources...")
    catalog.load_metadata()
    print()

    return catalog, duckdb_ds, postgres_ds


def execute_query(catalog, sql, description):
    """Execute a query and display results."""
    print(f"\n{'='*80}")
    print(f"EXAMPLE: {description}")
    print(f"{'='*80}")
    print(f"SQL:\n{sql}\n")

    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    try:
        ast = parser.parse(sql)
        logical_plan = parser.ast_to_logical_plan(ast)
        bound_plan = binder.bind(logical_plan)
        physical_plan = planner.plan(bound_plan)

        result_table = executor.execute_to_table(physical_plan)

        print(f"Results ({result_table.num_rows} rows):")
        print(result_table)
        print()
    except Exception as e:
        print(f"ERROR: {e}")
        print()


def run_single_source_examples(catalog, duckdb_name):
    """Run aggregation examples on single data source."""
    print("\n" + "="*80)
    print("SECTION 1: SINGLE-SOURCE AGGREGATIONS")
    print("="*80)

    execute_query(
        catalog,
        f"""
        SELECT COUNT(*) as total_products
        FROM {duckdb_name}.main.products
        """,
        "Simple COUNT - Total number of products"
    )

    execute_query(
        catalog,
        f"""
        SELECT
            category,
            COUNT(*) as product_count,
            AVG(unit_price) as avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price
        FROM {duckdb_name}.main.products
        GROUP BY category
        """,
        "GROUP BY category - Product statistics by category"
    )


def run_federated_aggregation_examples(catalog, duckdb_name, postgres_name):
    """Run federated JOIN + aggregation examples."""
    print("\n" + "="*80)
    print("SECTION 2: FEDERATED JOIN + AGGREGATION")
    print("="*80)

    execute_query(
        catalog,
        f"""
        SELECT
            p.category,
            COUNT(*) as order_count,
            SUM(o.quantity) as total_quantity,
            AVG(o.quantity) as avg_quantity
        FROM {postgres_name}.public.orders o
        JOIN {duckdb_name}.main.products p ON o.product_id = p.product_id
        GROUP BY p.category
        """,
        "Federated JOIN + GROUP BY - Orders by product category"
    )

    execute_query(
        catalog,
        f"""
        SELECT
            o.region,
            p.category,
            COUNT(*) as order_count,
            SUM(o.quantity) as total_items
        FROM {postgres_name}.public.orders o
        JOIN {duckdb_name}.main.products p ON o.product_id = p.product_id
        GROUP BY o.region, p.category
        """,
        "Multi-column GROUP BY - Orders by region and category"
    )

    execute_query(
        catalog,
        f"""
        SELECT
            p.product_name,
            COUNT(*) as times_ordered,
            SUM(o.quantity) as total_sold,
            AVG(o.quantity) as avg_order_size
        FROM {postgres_name}.public.orders o
        JOIN {duckdb_name}.main.products p ON o.product_id = p.product_id
        GROUP BY p.product_name
        """,
        "Product popularity - Aggregated sales by product"
    )

    execute_query(
        catalog,
        f"""
        SELECT
            o.region,
            COUNT(DISTINCT o.product_id) as unique_products,
            COUNT(*) as total_orders,
            SUM(o.quantity) as total_items
        FROM {postgres_name}.public.orders o
        JOIN {duckdb_name}.main.products p ON o.product_id = p.product_id
        WHERE p.category = 'Electronics'
        GROUP BY o.region
        """,
        "Filtered federated aggregation - Electronics sales by region"
    )


def show_aggregation_pushdown_examples(duckdb_name, postgres_name):
    """Show aggregation pushdown examples (future optimization)."""
    duckdb_label = duckdb_name or "DuckDB"
    postgres_label = postgres_name or "PostgreSQL"

    print("\n" + "="*80)
    print("SECTION 3: AGGREGATION PUSHDOWN (FUTURE OPTIMIZATION)")
    print("="*80)
    print("\nThe following queries demonstrate aggregation pushdown optimization,")
    print("which would push GROUP BY operations to the source database when possible.")
    print("This optimization is not yet implemented but is planned for Phase 4.")
    print()

    print("="*80)
    print("EXAMPLE: Single-source aggregation pushdown")
    print("="*80)
    print(f"""
Current behavior:
  1. Scan all rows from {postgres_label}.public.orders table
  2. Transfer all data to query engine
  3. Perform aggregation in query engine

Optimized behavior (with pushdown):
  1. Push aggregation to {postgres_label}
  2. {postgres_label} executes: SELECT region, COUNT(*), SUM(quantity)
                                FROM orders GROUP BY region
  3. Transfer only aggregated results (much smaller)

SQL:
SELECT
    region,
    COUNT(*) as order_count,
    SUM(quantity) as total_quantity
FROM {postgres_label}.public.orders
GROUP BY region

Benefits:
  - Reduces network transfer (only 4 rows instead of 10)
  - Leverages database's optimized aggregation
  - Lower memory usage in query engine
""")

    print("\n" + "="*80)
    print("EXAMPLE: Partial aggregation pushdown")
    print("="*80)
    print(f"""
Current behavior:
  1. Scan all orders from {postgres_label}
  2. Scan all products from {duckdb_label}
  3. JOIN in query engine
  4. Aggregate in query engine

Optimized behavior (with partial pushdown):
  1. Push partial aggregation to {postgres_label}:
     SELECT product_id, SUM(quantity) as qty_sum, COUNT(*) as cnt
     FROM orders GROUP BY product_id
  2. Transfer pre-aggregated data (5 rows instead of 10)
  3. JOIN with products table in {duckdb_label}
  4. Final aggregation in query engine

SQL:
SELECT
    p.category,
    SUM(o.quantity) as total_quantity,
    COUNT(*) as order_count
FROM {postgres_label}.public.orders o
JOIN {duckdb_label}.main.products p ON o.product_id = p.product_id
GROUP BY p.category

Benefits:
  - Reduces data transfer from {postgres_label} (5 rows instead of 10)
  - Some aggregation work offloaded to source database
  - More efficient when source has many rows per group
""")

    print("\n" + "="*80)
    print("EXAMPLE: Multi-stage aggregation pushdown")
    print("="*80)
    print(f"""
For queries with aggregations on both sides of a JOIN, we could push
aggregations to both sources:

SQL:
SELECT
    region_stats.region,
    region_stats.order_count,
    category_stats.category,
    category_stats.product_count
FROM
    (SELECT region, COUNT(*) as order_count
     FROM {postgres_label}.public.orders
     GROUP BY region) region_stats
CROSS JOIN
    (SELECT category, COUNT(*) as product_count
     FROM {duckdb_label}.main.products
     GROUP BY category) category_stats

This would push both GROUP BYs to their respective sources before the JOIN.

Implementation requirements:
  - Logical optimization rules to detect pushdown opportunities
  - Cost-based decision making (when is pushdown beneficial?)
  - Plan rewriting to insert remote aggregations
  - Partial aggregation support for complex cases
""")

    print("\n" + "="*80)
    print("NOTE: To enable aggregation pushdown in Phase 4, implement:")
    print("  1. AggregationPushdownRule in optimizer/rules.py")
    print("  2. Cost estimation for remote vs local aggregation")
    print("  3. Partial aggregation merge logic")
    print("  4. Capability detection (does datasource support aggregation?)")
    print("="*80)


def main():
    """Run example aggregation queries."""
    print("\n" + "="*80)
    print("FEDERATED QUERY ENGINE - PHASE 3 AGGREGATION EXAMPLES")
    print("="*80)
    print("\nThis example demonstrates:")
    print("  - Aggregations on single datasources (DuckDB)")
    print("  - Federated JOINs with aggregations (PostgreSQL + DuckDB)")
    print("  - Future aggregation pushdown optimization opportunities")
    print()

    catalog, duckdb_ds, postgres_ds = setup_federated_data()
    duckdb_name = duckdb_ds.name
    has_postgres = postgres_ds is not None
    postgres_name = postgres_ds.name if has_postgres else None

    print(f"\nSetup complete:")
    print(f"  - DuckDB ({duckdb_name}): products table (5 rows)")
    if has_postgres:
        print(f"  - PostgreSQL ({postgres_name}): orders table (10 rows)")
    else:
        print(f"  - PostgreSQL: NOT AVAILABLE")

    run_single_source_examples(catalog, duckdb_name)

    if has_postgres:
        run_federated_aggregation_examples(catalog, duckdb_name, postgres_name)
    else:
        print("\n" + "="*80)
        print("SECTION 2: FEDERATED JOIN + AGGREGATION")
        print("="*80)
        print("\nSkipping federated examples (PostgreSQL not available)")
        print("\nTo run federated examples, start PostgreSQL:")
        print("  docker-compose up -d postgres")

    show_aggregation_pushdown_examples(duckdb_name, postgres_name)

    print("\n" + "=" * 70)
    print("HAVING CLAUSE EXAMPLES")
    print("=" * 70)

    if has_postgres:
        execute_query(
            catalog,
            f"""
            SELECT
                region,
                COUNT(*) as order_count,
                SUM(quantity) as total_items
            FROM {postgres_name}.public.orders
            GROUP BY region
            HAVING COUNT(*) > 1
            """,
            "HAVING with COUNT - Regions with many orders"
        )

        execute_query(
            catalog,
            f"""
            SELECT
                product_id,
                SUM(quantity) as total_quantity,
                AVG(quantity) as avg_quantity
            FROM {postgres_name}.public.orders
            GROUP BY product_id
            HAVING SUM(quantity) >= 10
            """,
            "HAVING with SUM - High-volume products"
        )

        execute_query(
            catalog,
            f"""
            SELECT
                p.category,
                COUNT(*) as order_count,
                AVG(o.quantity) as avg_order_size
            FROM {postgres_name}.public.orders o
            JOIN {duckdb_name}.main.products p ON o.product_id = p.product_id
            GROUP BY p.category
            HAVING AVG(o.quantity) > 2
            """,
            "HAVING with AVG - Categories with large average orders"
        )
    else:
        print("Skipping HAVING clause examples (PostgreSQL not available)")

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("Phase 3 aggregation support includes:")
    print("  ✓ COUNT, SUM, AVG, MIN, MAX aggregate functions")
    print("  ✓ GROUP BY with single or multiple columns")
    print("  ✓ Aggregations without GROUP BY (global aggregates)")
    print("  ✓ Multiple aggregate functions in single query")
    print("  ✓ Hash-based aggregation for efficient execution")
    print("  ✓ Federated JOIN + aggregation (cross-database)")
    print("  ✓ HAVING clause for filtering aggregated results")
    print()
    print("Future optimizations:")
    print("  ⏳ Aggregation pushdown to source databases")
    print("  ⏳ Partial aggregation for complex queries")
    print("  ⏳ Cost-based aggregation strategy selection")
    print("="*80 + "\n")

    duckdb_ds.disconnect()
    if has_postgres:
        for ds in catalog.datasources.values():
            if isinstance(ds, PostgreSQLDataSource):
                ds.disconnect()


if __name__ == "__main__":
    main()
