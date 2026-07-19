"""Canonical tiny table library for the federated e2e correctness suite.

Each entry maps a bare table name to a spec: a portable ``CREATE TABLE``
statement and the ``INSERT`` statements that populate it. The DDL and inserts
use SQL that parses identically on DuckDB and PostgreSQL (INTEGER, BIGINT,
DOUBLE PRECISION, DECIMAL(10,2), VARCHAR, BOOLEAN, DATE, TIMESTAMP; DATE/TIMESTAMP
literals; TRUE/FALSE; quoted reserved-word identifiers). Every value is ASCII.

Tables are referenced from case queries by bare name inside ``{}`` placeholders;
the harness rewrites each placeholder to the placement's fully qualified name.
All keys line up so the trio (orders/products/customers) and the star
(fact_sales/dim_day/dim_item) join cleanly, while the ``t_*`` tables isolate one
edge each (null keys, duplicate keys, emptiness, every type, dates, text edges).
"""


class TableSpec:
    """One library table: its bare name, CREATE statement, and INSERT statements.

    ``inserts`` is a list so a zero-row table (``t_empty``) simply carries an
    empty list; the seeder runs the DDL then each insert in order.
    """

    def __init__(self, name, ddl, inserts):
        """Store the table's name, DDL text, and list of INSERT statements."""
        self.name = name
        self.ddl = ddl
        self.inserts = inserts


def _spec(name, ddl, inserts):
    """Build a TableSpec, defending the bare-name and ASCII invariants at load.

    Raising here means a malformed library entry fails at import, not silently
    mid-run. The DDL must mention the table name so seeding targets the intended
    relation, and every character must be ASCII.
    """
    if name not in ddl:
        raise ValueError("table '" + name + "' DDL does not mention its name")
    _require_ascii(name, ddl, inserts)
    return TableSpec(name=name, ddl=ddl, inserts=inserts)


def _require_ascii(name, ddl, inserts):
    """Raise if any DDL or insert text for a table contains a non-ASCII byte."""
    texts = [ddl]
    for insert in inserts:
        texts.append(insert)
    for text in texts:
        if not text.isascii():
            raise ValueError("table '" + name + "' has non-ASCII SQL text")


_ORDERS = _spec(
    "orders",
    "CREATE TABLE orders ("
    " order_id INTEGER, product_id INTEGER, customer_id INTEGER,"
    " quantity INTEGER, price DECIMAL(10,2), status VARCHAR, order_date DATE)",
    [
        "INSERT INTO orders VALUES"
        " (1, 101, 1, 3, 25.00, 'processing', DATE '2024-01-05'),"
        " (2, 102, 2, 5, 50.00, 'shipped', DATE '2024-02-10'),"
        " (3, 103, 1, 2, 75.00, 'processing', DATE '2024-02-20'),"
        " (4, 104, 3, 1, 125.00, 'returned', DATE '2024-03-01'),"
        " (5, 101, 2, 4, 60.00, 'processing', DATE '2024-03-15'),"
        " (6, 102, 1, 7, 35.00, 'shipped', DATE '2024-04-02'),"
        " (7, 108, 3, 3, 90.00, 'cancelled', DATE '2024-04-18'),"
        " (8, 104, 2, 6, 15.00, 'shipped', DATE '2024-05-09'),"
        " (9, 105, 5, 9, 10.00, 'processing', DATE '2024-05-22'),"
        " (10, 106, 5, 8, 200.00, 'processing', DATE '2024-06-01')"
    ],
)


_PRODUCTS = _spec(
    "products",
    "CREATE TABLE products ("
    " product_id INTEGER, name VARCHAR, category VARCHAR,"
    " unit_price DECIMAL(10,2), active BOOLEAN)",
    [
        "INSERT INTO products VALUES"
        " (101, 'jacket', 'clothing', 20.00, TRUE),"
        " (102, 'shirt', 'clothing', 30.00, TRUE),"
        " (103, 'tablet', 'electronics', 150.00, TRUE),"
        " (104, 'phone', 'electronics', 300.00, TRUE),"
        " (105, 'lamp', 'home', 40.00, FALSE),"
        " (106, 'desk', 'home', 220.00, TRUE),"
        " (107, 'chair', 'home', 90.00, TRUE),"
        " (108, 'coffee', 'food', 12.00, FALSE)"
    ],
)


_CUSTOMERS = _spec(
    "customers",
    "CREATE TABLE customers ("
    ' customer_id INTEGER, name VARCHAR, segment VARCHAR, city VARCHAR,'
    ' "group" VARCHAR)',
    [
        "INSERT INTO customers VALUES"
        " (1, 'Alice', 'enterprise', 'New York', 'g1'),"
        " (2, 'Bob', 'smb', 'London', 'g2'),"
        " (3, 'Cara', 'enterprise', 'Boston', 'g1'),"
        " (4, 'Dan', 'consumer', 'Paris', 'g3'),"
        " (5, 'Eve', 'consumer', 'Berlin', 'g2'),"
        " (6, 'Zoe', 'smb', 'Rome', 'g3')"
    ],
)


_FACT_SALES = _spec(
    "fact_sales",
    "CREATE TABLE fact_sales ("
    " sale_id INTEGER, day_key INTEGER, item_key INTEGER,"
    " amount DECIMAL(10,2), qty INTEGER)",
    [
        "INSERT INTO fact_sales VALUES"
        " (1, 20240101, 11, 100.00, 2),"
        " (2, 20240101, 12, 200.00, 1),"
        " (3, 20240102, 11, 150.00, 3),"
        " (4, 20240102, 13, 50.00, 5),"
        " (5, 20240103, 12, 300.00, 1),"
        " (6, 20240103, 13, 75.00, 4),"
        " (7, 20240103, 11, 25.00, 6)"
    ],
)


_DIM_DAY = _spec(
    "dim_day",
    "CREATE TABLE dim_day ("
    " day_key INTEGER, cal_date DATE, yr INTEGER, quarter INTEGER)",
    [
        "INSERT INTO dim_day VALUES"
        " (20240101, DATE '2024-01-01', 2024, 1),"
        " (20240102, DATE '2024-01-02', 2024, 1),"
        " (20240103, DATE '2024-01-03', 2024, 1)"
    ],
)


_DIM_ITEM = _spec(
    "dim_item",
    "CREATE TABLE dim_item ("
    " item_key INTEGER, item_name VARCHAR, dept VARCHAR)",
    [
        "INSERT INTO dim_item VALUES"
        " (11, 'widget', 'hardware'),"
        " (12, 'gadget', 'hardware'),"
        " (13, 'manual', 'media')"
    ],
)


_T_NULL_A = _spec(
    "t_null_a",
    "CREATE TABLE t_null_a (id INTEGER, k INTEGER, val VARCHAR)",
    [
        "INSERT INTO t_null_a VALUES"
        " (1, 10, 'a-ten'),"
        " (2, 20, 'a-twenty'),"
        " (3, NULL, 'a-null-one'),"
        " (4, NULL, 'a-null-two'),"
        " (5, 30, 'a-thirty')"
    ],
)


_T_NULL_B = _spec(
    "t_null_b",
    "CREATE TABLE t_null_b (id INTEGER, k INTEGER, label VARCHAR)",
    [
        "INSERT INTO t_null_b VALUES"
        " (1, 10, 'b-ten'),"
        " (2, NULL, 'b-null-one'),"
        " (3, 20, 'b-twenty'),"
        " (4, 40, 'b-forty')"
    ],
)


_T_DUP_A = _spec(
    "t_dup_a",
    "CREATE TABLE t_dup_a (k INTEGER, tag VARCHAR)",
    [
        "INSERT INTO t_dup_a VALUES"
        " (1, 'a1'),"
        " (1, 'a1-again'),"
        " (2, 'a2'),"
        " (2, 'a2-again'),"
        " (3, 'a3')"
    ],
)


_T_DUP_B = _spec(
    "t_dup_b",
    "CREATE TABLE t_dup_b (k INTEGER, note VARCHAR)",
    [
        "INSERT INTO t_dup_b VALUES"
        " (1, 'b1'),"
        " (1, 'b1-again'),"
        " (2, 'b2'),"
        " (4, 'b4')"
    ],
)


_T_EMPTY = _spec(
    "t_empty",
    "CREATE TABLE t_empty (id INTEGER, val VARCHAR)",
    [],
)


_T_TYPES = _spec(
    "t_types",
    "CREATE TABLE t_types ("
    " t_int INTEGER, t_big BIGINT, t_dbl DOUBLE PRECISION,"
    " t_dec DECIMAL(10,2), t_str VARCHAR, t_bool BOOLEAN,"
    " t_date DATE, t_ts TIMESTAMP)",
    [
        "INSERT INTO t_types VALUES"
        " (1, 9000000000, 2.5, 3.14, 'alpha', TRUE,"
        " DATE '2024-01-02', TIMESTAMP '2024-01-02 10:30:00'),"
        " (-2, -9000000000, -0.5, 0.00, 'beta', FALSE,"
        " DATE '1999-12-31', TIMESTAMP '1999-12-31 23:59:59'),"
        " (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    ],
)


_T_DATES = _spec(
    "t_dates",
    "CREATE TABLE t_dates (d_id INTEGER, d_date DATE, d_ts TIMESTAMP)",
    [
        "INSERT INTO t_dates VALUES"
        " (1, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00'),"
        " (2, DATE '2024-06-15', TIMESTAMP '2024-06-15 12:00:00'),"
        " (3, DATE '2023-02-28', TIMESTAMP '2023-02-28 06:15:30'),"
        " (4, DATE '2024-12-31', TIMESTAMP '2024-12-31 23:00:00')"
    ],
)


_T_TEXT = _spec(
    "t_text",
    "CREATE TABLE t_text (s_id INTEGER, s VARCHAR)",
    [
        "INSERT INTO t_text VALUES"
        " (1, ''),"
        " (2, '   '),"
        " (3, 'has''quote'),"
        " (4, 'per%cent'),"
        " (5, 'under_score'),"
        " (6, 'Mixed CASE'),"
        " (7, 'tab\tsep')"
    ],
)


_T_LOOKUP = _spec(
    "t_lookup",
    "CREATE TABLE t_lookup (code VARCHAR, description VARCHAR)",
    [
        "INSERT INTO t_lookup VALUES"
        " ('processing', 'in progress'),"
        " ('shipped', 'on the way'),"
        " ('returned', 'sent back'),"
        " ('cancelled', 'called off')"
    ],
)


def _build_library():
    """Assemble the name -> TableSpec library, rejecting any duplicate name."""
    specs = [
        _ORDERS, _PRODUCTS, _CUSTOMERS, _FACT_SALES, _DIM_DAY, _DIM_ITEM,
        _T_NULL_A, _T_NULL_B, _T_DUP_A, _T_DUP_B, _T_EMPTY, _T_TYPES,
        _T_DATES, _T_TEXT, _T_LOOKUP,
    ]
    library = {}
    for spec in specs:
        if spec.name in library:
            raise ValueError("duplicate table name '" + spec.name + "'")
        library[spec.name] = spec
    return library


TABLES = _build_library()


def get_spec(name):
    """Return the TableSpec for a library table, raising loudly if unknown."""
    if name not in TABLES:
        raise KeyError("unknown library table '" + name + "'")
    return TABLES[name]
