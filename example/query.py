"""Example demonstrating federated query engine setup with config and catalog."""

from pathlib import Path

from federated_query.catalog.catalog import Catalog
from federated_query.config.config import ExecutorConfig, load_config
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.executor.executor import Executor
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser


def print_pa_table(table):
    """Pretty-print a pyarrow Table."""
    if table.num_rows == 0:
        print("<empty>")
        return

    cols = [c.name for c in table.schema]
    rows = list(zip(*[table[c].to_pylist() for c in cols]))

    widths = [max(len(str(x)) for x in [name] + [r[i] for r in rows]) for i, name in enumerate(cols)]
    print(" | ".join(f"{name:<{widths[i]}}" for i, name in enumerate(cols)))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(" | ".join(f"{str(x):<{widths[i]}}" for i, x in enumerate(r)))


def execute_query(catalog, sql, description=None, *, show_sql=False, pretty=True):
    """Execute a query and display results."""
    if description:
        print(f"\n{'=' * 70}")
        print(f"EXAMPLE: {description}")
        print(f"{'=' * 70}")

    if show_sql or description:
        print("SQL:\n" + sql.strip() + "\n")

    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    ast = parser.parse(sql)
    logical_plan = parser.ast_to_logical_plan(ast)
    bound_plan = binder.bind(logical_plan)
    physical_plan = planner.plan(bound_plan)

    result_table = executor.execute_to_table(physical_plan)

    print(f"Results ({result_table.num_rows} rows):")
    if pretty:
        print_pa_table(result_table)
    else:
        print(result_table)
    print()


def create_postgres_sample_data(datasource):
    """Create sample tables in PostgreSQL for demonstration."""
    conn = datasource._get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS orders CASCADE")

            cursor.execute(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    amount DECIMAL(10,2),
                    region VARCHAR(50),
                    order_date DATE
                )
                """
            )

            cursor.execute(
                """
                INSERT INTO orders VALUES
                (101, 1, 1500.00, 'North', '2024-01-15'),
                (102, 2, 750.00, 'South', '2024-01-16'),
                (103, 1, 2200.00, 'North', '2024-01-17'),
                (104, 3, 1200.00, 'East', '2024-01-18'),
                (105, 2, 900.00, 'South', '2024-01-19'),
                (106, 1, 1800.00, 'North', '2024-01-20')
                """
            )

            conn.commit()
            print("  Created orders table in PostgreSQL")
    finally:
        datasource._return_connection(conn)


def create_duckdb_sample_data(datasource):
    """Create sample tables in DuckDB for demonstration."""
    conn = datasource.connection

    conn.execute("DROP TABLE IF EXISTS customers")

    conn.execute(
        """
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
        """
    )

    conn.execute(
        """
        INSERT INTO customers VALUES
        (1, 'Alice Smith', 'alice@example.com'),
        (2, 'Bob Jones', 'bob@example.com'),
        (3, 'Charlie Brown', 'charlie@example.com')
        """
    )

    print("  Created customers table in DuckDB")


def create_datasource(name, ds_config):
    """Create datasource instance from config."""
    ds_type = ds_config.type

    if ds_type == "duckdb":
        return DuckDBDataSource(name, ds_config.config)

    if ds_type == "postgresql":
        return PostgreSQLDataSource(name, ds_config.config)

    raise ValueError(f"Unsupported datasource type: {ds_type}")


def run_example_queries(catalog):
    """Run example queries demonstrating engine capabilities."""
    print("\n" + "=" * 70)
    print("RUNNING EXAMPLE QUERIES")
    print("=" * 70)

    execute_query(
        catalog,
        """
        SELECT c.name, c.id, o.amount, o.region
        FROM local_duckdb.main.customers c
        JOIN postgres_prod.public.orders o ON c.id = o.customer_id
        """,
        "Federated JOIN - Customers with their orders",
        show_sql=True,
    )

    execute_query(
        catalog,
        """
        SELECT c.name, o.amount
        FROM local_duckdb.main.customers c
        JOIN postgres_prod.public.orders o ON c.id = o.customer_id
        WHERE o.amount > 1000
        """,
        "JOIN with WHERE clause - High-value orders",
        show_sql=True,
    )

    execute_query(
        catalog,
        """
        SELECT
            region,
            COUNT(*) as order_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value
        FROM postgres_prod.public.orders
        GROUP BY region
        """,
        "Aggregation - Sales by region",
        show_sql=True,
    )

    execute_query(
        catalog,
        """
        SELECT
            c.name,
            COUNT(*) as order_count,
            SUM(o.amount) as total_spent
        FROM local_duckdb.main.customers c
        JOIN postgres_prod.public.orders o ON c.id = o.customer_id
        GROUP BY c.name
        """,
        "Federated JOIN + Aggregation - Customer spending summary",
        show_sql=True,
    )

    execute_query(
        catalog,
        """
        SELECT
            COUNT(*) as total_orders,
            SUM(amount) as total_revenue,
            AVG(amount) as average_order,
            MIN(amount) as smallest_order,
            MAX(amount) as largest_order
        FROM postgres_prod.public.orders
        """,
        "Global aggregation - Overall sales statistics",
        show_sql=True,
    )


def run_legacy_queries(catalog):
    """Replay legacy ad-hoc queries from the original example script."""
    legacy_queries = [
        (
            """
            SELECT c.name, c.id, o.amount
            FROM local_duckdb.main.customers c
            JOIN postgres_prod.public.orders o ON c.id = o.customer_id
            """,
            "Legacy join sample",
        ),
        (
            """
            SELECT count(*) AS order_count, O.customer_id
            FROM postgres_prod.public.orders O
            GROUP BY O.customer_id
            """,
            "Legacy customer order counts",
        ),
    ]

    for sql, title in legacy_queries:
        execute_query(
            catalog,
            sql,
            description=f"Legacy: {title}",
            show_sql=True,
            pretty=True,
        )

    experimental_queries = [
        (
            """
            SELECT c.name, c.id, sum(o.amount)
            FROM local_duckdb.main.customers c
            JOIN postgres_prod.public.orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            """,
            "Legacy: customer spend totals",
        ),
        (
            """
            SELECT C.name,
                (
                    SELECT count(*)
                    FROM postgres_prod.public.orders O
                    WHERE O.customer_id = C.id
                    GROUP BY O.customer_id
                )
            FROM local_duckdb.main.customers C
            """,
            "Legacy: correlated subquery counts",
        ),
    ]

    for sql, title in experimental_queries:
        print(
            f"Skipping '{title}' (manual verification required). "
            "Uncomment execute_query calls in run_legacy_queries once supported."
        )
        # To run manually, uncomment below line:
        # execute_query(catalog, sql, description=title, show_sql=True)


def main():
    """Main example function."""
    config_path = Path(__file__).parent / "dbconfig.yaml"

    config = load_config(str(config_path))

    print(f"Loaded config with {len(config.datasources)} datasource(s)")
    print()

    catalog = Catalog()

    for ds_name, ds_config in config.datasources.items():
        print(f"Setting up datasource: {ds_name} ({ds_config.type})")

        try:
            datasource = create_datasource(ds_name, ds_config)
            datasource.connect()

            if ds_config.type == "postgresql":
                create_postgres_sample_data(datasource)
            elif ds_config.type == "duckdb":
                create_duckdb_sample_data(datasource)

            catalog.register_datasource(datasource)
            print(f"  Successfully registered {ds_name}")

        except Exception as e:
            print(f"  WARNING: Could not connect to {ds_name}: {e}")
            print(f"  Skipping {ds_name}...")
            continue

        print()

    if not catalog.datasources:
        print("ERROR: No datasources were successfully registered!")
        return

    print("Loading metadata from datasources...")
    catalog.load_metadata()

    print(f"\nCatalog summary: {catalog}")

    print("\nSchemas and tables in catalog:")
    for key, schema in catalog.schemas.items():
        ds_name, schema_name = key
        if schema.tables:
            print(f"\n  {ds_name}.{schema_name}:")
            for table in schema.tables.values():
                print(f"    {table.name}:")
                for col in table.columns:
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    print(f"      - {col.name}: {col.data_type.value} {nullable}")

    print("\n" + "=" * 60)
    print("Federated catalog setup complete!")
    print("=" * 60)
    print(f"Total datasources: {len(catalog.datasources)}")
    print(f"Total schemas: {len(catalog.schemas)}")

    total_tables = sum(len(s.tables) for s in catalog.schemas.values())
    print(f"Total tables: {total_tables}")

    print("\nYou can now execute federated queries across:")
    for ds_name in catalog.datasources.keys():
        print(f"  - {ds_name}")

    run_example_queries(catalog)
    run_legacy_queries(catalog)

    print("\nCleaning up connections...")
    for datasource in catalog.datasources.values():
        try:
            datasource.disconnect()
        except Exception as e:
            print(f"  Error disconnecting {datasource.name}: {e}")


if __name__ == "__main__":
    main()
