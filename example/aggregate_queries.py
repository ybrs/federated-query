"""Example aggregation queries demonstrating Phase 3 functionality."""

from pathlib import Path
from federated_query.config.config import load_config
from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.parser import Parser, Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor
from federated_query.config.config import ExecutorConfig


def setup_example_data():
    """Set up in-memory DuckDB with example data."""
    config = {
        "database": ":memory:",
        "read_only": False,
    }
    datasource = DuckDBDataSource(name="analytics_db", config=config)
    datasource.connect()

    datasource.connection.execute("""
        CREATE TABLE sales (
            id INTEGER,
            region VARCHAR,
            product VARCHAR,
            amount DOUBLE,
            quantity INTEGER,
            sale_date DATE
        )
    """)

    datasource.connection.execute("""
        INSERT INTO sales VALUES
        (1, 'North', 'Widget', 100.0, 5, '2024-01-15'),
        (2, 'South', 'Gadget', 200.0, 10, '2024-01-16'),
        (3, 'North', 'Widget', 150.0, 8, '2024-01-17'),
        (4, 'East', 'Gadget', 300.0, 15, '2024-01-18'),
        (5, 'South', 'Widget', 250.0, 12, '2024-01-19'),
        (6, 'North', 'Gadget', 180.0, 9, '2024-01-20'),
        (7, 'East', 'Widget', 220.0, 11, '2024-01-21'),
        (8, 'West', 'Gadget', 175.0, 7, '2024-01-22'),
        (9, 'West', 'Widget', 195.0, 9, '2024-01-23'),
        (10, 'North', 'Gadget', 210.0, 10, '2024-01-24')
    """)

    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    return catalog, datasource


def execute_query(catalog, sql, description):
    """Execute a query and display results."""
    print(f"\n{'='*80}")
    print(f"QUERY: {description}")
    print(f"{'='*80}")
    print(f"SQL:\n{sql}\n")

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
    print(result_table)


def main():
    """Run example aggregation queries."""
    print("\n" + "="*80)
    print("FEDERATED QUERY ENGINE - PHASE 3 AGGREGATION EXAMPLES")
    print("="*80)

    catalog, datasource = setup_example_data()

    execute_query(
        catalog,
        """
        SELECT COUNT(*) as total_sales
        FROM analytics_db.main.sales
        """,
        "Example 1: Simple COUNT(*) - Total number of sales"
    )

    execute_query(
        catalog,
        """
        SELECT
            region,
            COUNT(*) as num_sales,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_sale_amount
        FROM analytics_db.main.sales
        GROUP BY region
        """,
        "Example 2: GROUP BY with multiple aggregates - Sales by region"
    )

    execute_query(
        catalog,
        """
        SELECT
            product,
            COUNT(*) as num_sales,
            SUM(quantity) as total_quantity,
            MIN(amount) as min_sale,
            MAX(amount) as max_sale
        FROM analytics_db.main.sales
        GROUP BY product
        """,
        "Example 3: Product sales summary with MIN/MAX"
    )

    execute_query(
        catalog,
        """
        SELECT
            region,
            product,
            COUNT(*) as sales_count,
            AVG(amount) as avg_amount
        FROM analytics_db.main.sales
        GROUP BY region, product
        """,
        "Example 4: GROUP BY multiple columns - Sales by region and product"
    )

    execute_query(
        catalog,
        """
        SELECT
            SUM(amount) as total_revenue,
            AVG(amount) as average_sale,
            MIN(amount) as smallest_sale,
            MAX(amount) as largest_sale,
            COUNT(*) as total_transactions
        FROM analytics_db.main.sales
        """,
        "Example 5: Global aggregation (no GROUP BY) - Overall sales statistics"
    )

    execute_query(
        catalog,
        """
        SELECT
            region,
            SUM(amount) as revenue
        FROM analytics_db.main.sales
        GROUP BY region
        """,
        "Example 6: Simple SUM aggregation by region"
    )

    execute_query(
        catalog,
        """
        SELECT
            product,
            AVG(quantity) as avg_quantity_per_sale
        FROM analytics_db.main.sales
        GROUP BY product
        """,
        "Example 7: AVG aggregation - Average quantities by product"
    )

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("Phase 3 aggregation support includes:")
    print("  ✓ COUNT, SUM, AVG, MIN, MAX aggregate functions")
    print("  ✓ GROUP BY with single or multiple columns")
    print("  ✓ Aggregations without GROUP BY (global aggregates)")
    print("  ✓ Multiple aggregate functions in single query")
    print("  ✓ Hash-based aggregation for efficient execution")
    print("\nNote: HAVING clause parsing is implemented but evaluation ")
    print("      requires column name translation (future enhancement)")
    print("="*80 + "\n")

    datasource.disconnect()


if __name__ == "__main__":
    main()
