#!/usr/bin/env python3
"""Initialize DuckDB with sample data for testing."""

import duckdb
from pathlib import Path


def init_duckdb(db_path: str = "data/test.duckdb"):
    """Initialize DuckDB database with sample data.

    Args:
        db_path: Path to DuckDB database file
    """
    # Create data directory if it doesn't exist
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect(str(db_file))

    print(f"Initializing DuckDB at {db_path}...")

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS main")

    # Create customers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL,
            country VARCHAR,
            signup_date DATE
        )
    """)

    # Create departments table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.departments (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            location VARCHAR
        )
    """)

    # Create employees table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            department_id INTEGER,
            salary DECIMAL(10, 2),
            hire_date DATE
        )
    """)

    # Insert sample customers
    conn.execute("""
        INSERT INTO main.customers (id, name, email, country, signup_date) VALUES
            (1, 'Alice Johnson', 'alice@example.com', 'USA', '2023-01-15'),
            (2, 'Bob Smith', 'bob@example.com', 'USA', '2023-02-20'),
            (3, 'Carol White', 'carol@example.com', 'UK', '2023-01-18'),
            (4, 'David Brown', 'david@example.com', 'Germany', '2023-02-05'),
            (5, 'Eve Davis', 'eve@example.com', 'Japan', '2023-01-30'),
            (6, 'Frank Wilson', 'frank@example.com', 'Australia', '2023-02-28'),
            (7, 'Grace Lee', 'grace@example.com', 'USA', '2023-03-15'),
            (8, 'Henry Taylor', 'henry@example.com', 'UK', '2023-01-25'),
            (9, 'Ivy Martinez', 'ivy@example.com', 'USA', '2023-02-18'),
            (10, 'Jack Anderson', 'jack@example.com', 'Canada', '2023-03-05')
    """)

    # Insert sample departments
    conn.execute("""
        INSERT INTO main.departments (id, name, location) VALUES
            (1, 'Engineering', 'San Francisco'),
            (2, 'Sales', 'New York'),
            (3, 'Marketing', 'London'),
            (4, 'HR', 'Tokyo'),
            (5, 'Finance', 'Singapore')
    """)

    # Insert sample employees
    conn.execute("""
        INSERT INTO main.employees (id, name, department_id, salary, hire_date) VALUES
            (1, 'John Doe', 1, 120000.00, '2022-01-10'),
            (2, 'Jane Smith', 1, 115000.00, '2022-03-15'),
            (3, 'Mike Johnson', 2, 95000.00, '2021-06-20'),
            (4, 'Sarah Williams', 2, 98000.00, '2022-02-28'),
            (5, 'Tom Brown', 3, 88000.00, '2022-05-12'),
            (6, 'Lisa Davis', 3, 92000.00, '2021-11-30'),
            (7, 'James Wilson', 4, 75000.00, '2023-01-15'),
            (8, 'Emily Taylor', 4, 73000.00, '2022-09-01'),
            (9, 'Robert Martinez', 5, 105000.00, '2021-08-20'),
            (10, 'Maria Garcia', 5, 110000.00, '2022-04-18')
    """)

    # Analyze tables
    conn.execute("ANALYZE main.customers")
    conn.execute("ANALYZE main.departments")
    conn.execute("ANALYZE main.employees")

    # Verify data
    customer_count = conn.execute("SELECT COUNT(*) FROM main.customers").fetchone()[0]
    department_count = conn.execute("SELECT COUNT(*) FROM main.departments").fetchone()[0]
    employee_count = conn.execute("SELECT COUNT(*) FROM main.employees").fetchone()[0]

    print(f"✓ Created {customer_count} customers")
    print(f"✓ Created {department_count} departments")
    print(f"✓ Created {employee_count} employees")

    conn.close()
    print(f"\nDuckDB initialized successfully at {db_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Initialize DuckDB with sample data")
    parser.add_argument(
        "--path",
        default="data/test.duckdb",
        help="Path to DuckDB database file (default: data/test.duckdb)",
    )
    args = parser.parse_args()

    init_duckdb(args.path)
