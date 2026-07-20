"""Typed SQL generator for the federated fuzzer, as hypothesis strategies.

The generator reads the e2e table library (``tests/e2e_federated/tables.py``),
derives each table's column/type map by parsing its ``CREATE TABLE`` DDL, and
emits SQL biased toward federated-interesting shapes: multi-table joins, corre-
lated and uncorrelated subqueries, set operations, aggregates, window functions,
CTEs, and derived tables. Every table is referenced as a ``{table_name}`` place-
holder exactly like a corpus case, so the e2e harness's per-placement
qualification substitutes the right three-part names.

The surface is kept to what the engine and a single-DuckDB oracle agree on:
boolean columns are never placed in WHERE/GROUP BY/JOIN-ON (they trip a known
PostgreSQL stats-probe bug), integer division and bare LIMIT-without-total-order
are never generated (dialect / nondeterminism), and every projected expression
is aliased so engine and oracle column names always match. A generated query
that the oracle rejects is discarded by the oracle stack, not here.
"""

import sqlglot
from hypothesis import strategies as st

from tests.e2e_federated import tables as table_library

_RESERVED_COLUMNS = frozenset(["group"])


def _map_category(type_sql):
    """Map a DDL type string to one of the generator's type categories."""
    upper = type_sql.upper()
    for prefix, category in _CATEGORY_PREFIXES:
        if upper.startswith(prefix):
            return category
    raise ValueError("unmapped DDL type '" + type_sql + "'")


_CATEGORY_PREFIXES = (
    ("BIGINT", "bigint"),
    ("INT", "int"),
    ("DECIMAL", "decimal"),
    ("DOUBLE", "double"),
    ("TEXT", "str"),
    ("VARCHAR", "str"),
    ("BOOLEAN", "bool"),
    ("TIMESTAMP", "ts"),
    ("DATE", "date"),
)


class Column:
    """One table column: its owning table, name, and generator type category."""

    def __init__(self, table, name, category):
        """Store the column's table, name, and type category."""
        self.table = table
        self.name = name
        self.category = category


class Table:
    """One library table modeled as an ordered list of typed columns."""

    def __init__(self, name, columns):
        """Store the table name and its Column list."""
        self.name = name
        self.columns = columns


def _parse_table(name):
    """Parse a library table's DDL into a typed Table model."""
    spec = table_library.get_spec(name)
    ast = sqlglot.parse_one(spec.ddl, read="duckdb")
    columns = []
    for definition in ast.find_all(sqlglot.expressions.ColumnDef):
        category = _map_category(definition.args["kind"].sql())
        columns.append(Column(name, definition.name, category))
    return Table(name, columns)


def _build_schema():
    """Build the name -> Table map for every library table."""
    schema = {}
    for name in table_library.TABLES:
        schema[name] = _parse_table(name)
    return schema


SCHEMA = _build_schema()


# Join edges connect two tables on a same-category column pair. The generator
# builds join chains by walking these edges, so every equi-join it emits joins
# real, type-compatible keys rather than a random Cartesian pairing.
JOIN_EDGES = (
    ("orders", "product_id", "products", "product_id"),
    ("orders", "customer_id", "customers", "customer_id"),
    ("fact_sales", "item_key", "dim_item", "item_key"),
    ("fact_sales", "day_key", "dim_day", "day_key"),
    ("orders", "status", "t_lookup", "code"),
    ("t_null_a", "k", "t_null_b", "k"),
    ("t_null_a", "id", "t_null_b", "id"),
    ("t_dup_a", "k", "t_dup_b", "k"),
    ("orders", "price", "products", "unit_price"),
)


# Curated selective string domains, keyed (table, column). A string predicate
# draws its literal from the column's real values so it filters meaningfully;
# string columns without a domain here are left out of predicate generation.
STRING_DOMAINS = {
    ("orders", "status"): ("processing", "shipped", "returned", "cancelled"),
    ("customers", "city"): ("New York", "London", "Boston", "Paris", "Berlin"),
    ("customers", "segment"): ("enterprise", "smb", "consumer"),
    ("customers", "name"): ("Alice", "Bob", "Cara", "Dan", "Eve", "Zoe"),
    ("products", "name"): ("jacket", "shirt", "tablet", "phone", "lamp"),
    ("products", "category"): ("clothing", "electronics", "home", "food"),
    ("dim_item", "item_name"): ("widget", "gadget", "manual"),
    ("dim_item", "dept"): ("hardware", "media"),
    ("t_lookup", "code"): ("processing", "shipped", "returned", "cancelled"),
}

_INT_POOL = (-2, 0, 1, 2, 3, 5, 11, 12, 20, 101, 104, 20240101)
_DECIMAL_POOL = ("10.00", "20.00", "50.00", "75.00", "125.00", "200.00")
_DOUBLE_POOL = ("-0.5", "0.0", "2.5", "3.14")
_BIGINT_POOL = (-9000000000, 0, 1, 9000000000)
_DATE_POOL = ("2024-01-01", "2024-02-10", "2024-03-01", "2024-12-31")
_TS_POOL = ("2024-01-01 00:00:00", "2024-06-15 12:00:00", "2024-12-31 23:00:00")


def _is_predicate_safe(column):
    """Whether a column may appear in WHERE/GROUP BY/JOIN-ON without a known bug.

    Booleans are excluded (a boolean in WHERE/GROUP BY on a PostgreSQL source
    trips the engine's stats-probe bug), reserved-word columns are excluded (they
    need quoting), and a string column is allowed only when it has a curated
    selective domain.
    """
    if column.category == "bool" or column.name in _RESERVED_COLUMNS:
        return False
    if column.category == "str":
        return (column.table, column.name) in STRING_DOMAINS
    return True


def _selectable_columns(table_name):
    """Return the columns of a table that may be projected (drop reserved)."""
    columns = []
    for column in SCHEMA[table_name].columns:
        if column.name not in _RESERVED_COLUMNS:
            columns.append(column)
    return columns


def _predicate_columns(table_name):
    """Return the predicate-safe columns of a table."""
    columns = []
    for column in _selectable_columns(table_name):
        if _is_predicate_safe(column):
            columns.append(column)
    return columns


class GenQuery:
    """One generated query and the metadata the oracle stack needs to check it.

    ``sql`` carries ``{table}`` placeholders and bare aliases; ``tables`` is the
    distinct library tables it uses; ``shape`` names its family for stats;
    ``order_sensitive`` is True only when the query ends in a total-order ORDER
    BY; ``tlp`` is a TlpSpec when the shape supports ternary-predicate splitting.
    """

    def __init__(self, sql, tables, shape, order_sensitive=False, tlp=None):
        """Store the SQL, its tables, shape name, order flag, and TLP spec."""
        self.sql = sql
        self.tables = tables
        self.shape = shape
        self.order_sensitive = order_sensitive
        self.tlp = tlp


class TlpSpec:
    """The parts a TLP check needs to re-partition a plain SELECT by a predicate.

    ``proj`` and ``from_sql`` render the query body; ``predicate_columns`` are
    (colref, category) candidates for building the partition predicate, and
    ``anchor_table`` is the FROM anchor so TLP can prefer a different source.
    """

    def __init__(self, proj, from_sql, predicate_columns, anchor_table):
        """Store the projection, FROM text, predicate columns, and anchor."""
        self.proj = proj
        self.from_sql = from_sql
        self.predicate_columns = predicate_columns
        self.anchor_table = anchor_table


def _quote_string(value):
    """Render a Python string as a single-quoted SQL literal (doubling quotes)."""
    return "'" + value.replace("'", "''") + "'"


def _literal_strategy(column):
    """Return a hypothesis strategy of SQL-literal strings for a column's type."""
    category = column.category
    if category == "str":
        return _string_literal_strategy(column)
    return _scalar_literal_strategy(category)


def _string_literal_strategy(column):
    """Draw a selective string literal from the column's curated domain."""
    domain = STRING_DOMAINS[(column.table, column.name)]
    return st.sampled_from(domain).map(_quote_string)


def _scalar_literal_strategy(category):
    """Return the literal strategy for a non-string type category."""
    builder = _SCALAR_LITERALS.get(category)
    if builder is None:
        raise ValueError("no literal strategy for category '" + category + "'")
    return builder()


_SCALAR_LITERALS = {
    "int": lambda: st.sampled_from(_INT_POOL).map(str),
    "bigint": lambda: st.sampled_from(_BIGINT_POOL).map(str),
    "decimal": lambda: st.sampled_from(_DECIMAL_POOL),
    "double": lambda: st.sampled_from(_DOUBLE_POOL),
    "date": lambda: st.sampled_from(_DATE_POOL).map(lambda v: "DATE '" + v + "'"),
    "ts": lambda: st.sampled_from(_TS_POOL).map(lambda v: "TIMESTAMP '" + v + "'"),
}


_NUMERIC = frozenset(["int", "bigint", "decimal", "double"])
_ORDERED = frozenset(["int", "bigint", "decimal", "double", "date", "ts"])


@st.composite
def _comparison_predicate(draw, colref, column):
    """Build a ``colref OP literal`` predicate valid for the column's type."""
    operators = _operators_for(column.category)
    operator = draw(st.sampled_from(operators))
    literal = draw(_literal_strategy(column))
    return colref + " " + operator + " " + literal


def _operators_for(category):
    """Return the comparison operators valid for a type category."""
    if category in _ORDERED and category != "double":
        return ("=", "<>", "<", ">", "<=", ">=")
    if category == "double":
        return ("<", ">", "<=", ">=")
    return ("=", "<>")


@st.composite
def _predicate(draw, colref, column):
    """Draw a ternary predicate over one column (comparison or NULL test)."""
    kind = draw(st.sampled_from(("cmp", "cmp", "null", "notnull")))
    if kind == "null":
        return colref + " IS NULL"
    if kind == "notnull":
        return colref + " IS NOT NULL"
    return draw(_comparison_predicate(colref, column))


def _aliases_for(tables):
    """Return a table -> stable alias map (``r0``, ``r1``, ...)."""
    aliases = {}
    for index, name in enumerate(tables):
        aliases[name] = "r" + str(index)
    return aliases


def _colref(alias, column):
    """Return the aliased column reference ``alias.column``."""
    return alias + "." + column.name


@st.composite
def _projection(draw, choices, count):
    """Draw ``count`` aliased projection items from (alias, column) choices."""
    picks = draw(st.lists(st.sampled_from(choices), min_size=count, max_size=count))
    items = []
    for index, (alias, column) in enumerate(picks):
        items.append(_colref(alias, column) + " AS c" + str(index))
    return ", ".join(items)


def _choice_columns(tables, aliases):
    """Return every (alias, column) projectable pair across the given tables."""
    choices = []
    for name in tables:
        for column in _selectable_columns(name):
            choices.append((aliases[name], column))
    return choices


def _predicate_choices(tables, aliases):
    """Return every (alias, column) predicate-safe pair across the tables."""
    choices = []
    for name in tables:
        for column in _predicate_columns(name):
            choices.append((aliases[name], column))
    return choices


@st.composite
def _where_clause(draw, choices):
    """Draw a conjunction of one or two per-column predicates, or empty text."""
    if not choices:
        return ""
    count = draw(st.integers(min_value=0, max_value=2))
    if count == 0:
        return ""
    parts = draw(_predicate_conjuncts(choices, count))
    return " WHERE " + " AND ".join(parts)


@st.composite
def _predicate_conjuncts(draw, choices, count):
    """Draw ``count`` predicate strings from the predicate-column choices."""
    parts = []
    for _index in range(count):
        alias, column = draw(st.sampled_from(choices))
        parts.append(draw(_predicate(_colref(alias, column), column)))
    return parts


def _placeholder(name, alias):
    """Return the ``{name} alias`` FROM fragment for a table."""
    return "{" + name + "} " + alias


@st.composite
def project_query(draw):
    """Generate a single-table projection: ``SELECT cols FROM t [WHERE p]``.

    This shape is TLP-able: it carries no DISTINCT/GROUP BY/LIMIT, so splitting
    its WHERE by a ternary predicate partitions its rows exactly.
    """
    name = draw(st.sampled_from(sorted(SCHEMA)))
    aliases = _aliases_for([name])
    choices = _choice_columns([name], aliases)
    count = draw(st.integers(min_value=1, max_value=min(3, len(choices))))
    proj = draw(_projection(choices, count))
    from_sql = _placeholder(name, aliases[name])
    tlp = _tlp_spec(proj, from_sql, [name], aliases)
    return GenQuery("SELECT " + proj + " FROM " + from_sql, [name], "project", tlp=tlp)


def _tlp_spec(proj, from_sql, tables, aliases):
    """Build a TlpSpec when any predicate-safe column exists, else None."""
    choices = _predicate_choices(tables, aliases)
    if not choices:
        return None
    columns = []
    for alias, column in choices:
        columns.append((_colref(alias, column), column))
    return TlpSpec(proj, from_sql, columns, tables[0])


@st.composite
def filter_query(draw):
    """Generate a single-table filtered projection (a WHERE-bearing project)."""
    name = draw(st.sampled_from(sorted(SCHEMA)))
    aliases = _aliases_for([name])
    proj_choices = _choice_columns([name], aliases)
    pred_choices = _predicate_choices([name], aliases)
    if not pred_choices:
        return draw(project_query())
    proj = draw(
        _projection(proj_choices, draw(st.integers(1, min(3, len(proj_choices)))))
    )
    where = draw(_where_clause(pred_choices))
    from_sql = _placeholder(name, aliases[name])
    tlp = _tlp_spec(proj, from_sql, [name], aliases)
    sql = "SELECT " + proj + " FROM " + from_sql + where
    return GenQuery(sql, [name], "filter", tlp=tlp)


def _neighbors(name):
    """Return edges touching a table as (other_table, this_col, other_col)."""
    found = []
    for left, left_col, right, right_col in JOIN_EDGES:
        if left == name:
            found.append((right, left_col, right_col))
        elif right == name:
            found.append((left, right_col, left_col))
    return found


@st.composite
def _join_plan(draw, size):
    """Grow a connected join plan up to ``size`` tables via the edge graph.

    Returns (ordered tables, list of (new_table, this_col, that_table, that_col,
    join_type)); the plan stops early if no edge extends the current set.
    """
    start = draw(st.sampled_from(sorted(SCHEMA)))
    tables = [start]
    joins = []
    for _step in range(size - 1):
        added = draw(_extend_join(tables))
        if added is None:
            break
        joins.append(added)
        tables.append(added[0])
    return tables, joins


@st.composite
def _extend_join(draw, tables):
    """Draw one edge extending the current table set, or None if none applies."""
    options = _extension_options(tables)
    if not options:
        return None
    base_table, this_col, other, other_col = draw(st.sampled_from(options))
    join_type = draw(st.sampled_from(("INNER", "INNER", "LEFT", "RIGHT", "FULL")))
    return (other, this_col, base_table, other_col, join_type)


def _extension_options(tables):
    """Return edges from a table already in the set to a table not yet in it."""
    present = set(tables)
    options = []
    for name in tables:
        for other, this_col, other_col in _neighbors(name):
            if other not in present:
                options.append((name, this_col, other, other_col))
    return options


def _render_from(tables, joins, aliases):
    """Render the FROM/JOIN text for a join plan."""
    parts = [_placeholder(tables[0], aliases[tables[0]])]
    for new_table, this_col, base_table, other_col, join_type in joins:
        on_left = _colref(aliases[base_table], _named(base_table, this_col))
        on_right = _colref(aliases[new_table], _named(new_table, other_col))
        clause = join_type + " JOIN " + _placeholder(new_table, aliases[new_table])
        parts.append(clause + " ON " + on_left + " = " + on_right)
    return " ".join(parts)


def _named(table_name, column_name):
    """Return the Column object for a table/column name pair."""
    for column in SCHEMA[table_name].columns:
        if column.name == column_name:
            return column
    raise KeyError("no column '" + column_name + "' on '" + table_name + "'")


@st.composite
def join_query(draw):
    """Generate a 2-5 table join with mixed join types and per-source filters."""
    size = draw(st.integers(min_value=2, max_value=5))
    tables, joins = draw(_join_plan(size))
    aliases = _aliases_for(tables)
    proj_choices = _choice_columns(tables, aliases)
    count = draw(st.integers(min_value=1, max_value=min(4, len(proj_choices))))
    proj = draw(_projection(proj_choices, count))
    where = draw(_where_clause(_predicate_choices(tables, aliases)))
    from_sql = _render_from(tables, joins, aliases)
    sql = "SELECT " + proj + " FROM " + from_sql + where
    tlp = _tlp_join_spec(proj, tables, joins, aliases, bool(where))
    return GenQuery(sql, tables, "join", tlp=tlp)


def _tlp_join_spec(proj, tables, joins, aliases, has_where):
    """Build a TlpSpec for an inner-only, WHERE-free join (safe to split)."""
    if has_where:
        return None
    for _new, _tc, _bt, _oc, join_type in joins:
        if join_type != "INNER":
            return None
    from_sql = _render_from(tables, joins, aliases)
    return _tlp_spec(proj, from_sql, tables, aliases)


_AGG_FUNCS = ("SUM", "COUNT", "MIN", "MAX", "AVG")


@st.composite
def aggregate_query(draw):
    """Generate a GROUP BY aggregate over a join, with FILTER/DISTINCT/HAVING."""
    tables, joins = draw(_join_plan(draw(st.integers(2, 3))))
    aliases = _aliases_for(tables)
    group = draw(
        st.sampled_from(
            _predicate_choices(tables, aliases) or _choice_columns(tables, aliases)
        )
    )
    metric = draw(_aggregate_item(tables, aliases))
    group_ref = _colref(group[0], group[1])
    body = "SELECT " + group_ref + " AS g0, " + metric + " FROM "
    body += _render_from(tables, joins, aliases) + " GROUP BY " + group_ref
    body += draw(_maybe_having())
    return GenQuery(body, tables, "aggregate")


@st.composite
def _aggregate_item(draw, tables, aliases):
    """Draw one aliased aggregate call, optionally DISTINCT or with a FILTER."""
    func = draw(st.sampled_from(_AGG_FUNCS))
    if func == "COUNT" and draw(st.booleans()):
        return "COUNT(*)" + draw(_maybe_filter(tables, aliases)) + " AS m0"
    alias, column = draw(st.sampled_from(_numeric_choices(tables, aliases)))
    distinct = "DISTINCT " if draw(st.booleans()) else ""
    call = func + "(" + distinct + _colref(alias, column) + ")"
    return call + draw(_maybe_filter(tables, aliases)) + " AS m0"


def _numeric_choices(tables, aliases):
    """Return (alias, column) pairs whose column is numeric, for aggregation."""
    choices = []
    for name in tables:
        for column in _selectable_columns(name):
            if column.category in _NUMERIC:
                choices.append((aliases[name], column))
    return choices


@st.composite
def _maybe_filter(draw, tables, aliases):
    """Draw an optional ``FILTER (WHERE p)`` clause for an aggregate."""
    choices = _predicate_choices(tables, aliases)
    if not choices or not draw(st.booleans()):
        return ""
    alias, column = draw(st.sampled_from(choices))
    predicate = draw(_comparison_predicate(_colref(alias, column), column))
    return " FILTER (WHERE " + predicate + ")"


@st.composite
def _maybe_having(draw):
    """Draw an optional HAVING clause on the aggregate output."""
    if not draw(st.booleans()):
        return ""
    threshold = draw(st.sampled_from((0, 1, 2, 5)))
    return " HAVING COUNT(*) > " + str(threshold)


@st.composite
def setop_query(draw):
    """Generate a set operation over two structurally aligned branches."""
    edge = draw(st.sampled_from(JOIN_EDGES))
    left_name, left_col, right_name, right_col = edge
    operator = draw(st.sampled_from(("UNION", "UNION ALL", "INTERSECT", "EXCEPT")))
    left = "SELECT " + left_col + " AS c0 FROM {" + left_name + "}"
    right = "SELECT " + right_col + " AS c0 FROM {" + right_name + "}"
    sql = left + " " + operator + " " + right
    return GenQuery(sql, sorted({left_name, right_name}), "setop")


@st.composite
def subquery_query(draw):
    """Generate an EXISTS/IN/scalar subquery correlated on a join edge."""
    edge = draw(st.sampled_from(JOIN_EDGES))
    kind = draw(st.sampled_from(("exists", "not_exists", "in", "scalar")))
    return _subquery_of_kind(edge, kind)


def _subquery_of_kind(edge, kind):
    """Render one subquery shape for an edge and a chosen kind."""
    outer, outer_col, inner, inner_col = edge
    tables = sorted({outer, inner})
    if kind == "scalar":
        return _scalar_subquery(outer, outer_col, inner, inner_col, tables)
    return _predicate_subquery(kind, outer, outer_col, inner, inner_col, tables)


def _predicate_subquery(kind, outer, outer_col, inner, inner_col, tables):
    """Render an EXISTS/NOT EXISTS/IN subquery over an edge."""
    if kind == "in":
        body = (
            "SELECT r0."
            + outer_col
            + " AS c0 FROM {"
            + outer
            + "} r0 WHERE r0."
            + outer_col
        )
        body += " IN (SELECT " + inner_col + " FROM {" + inner + "})"
        return GenQuery(body, tables, "subquery")
    negate = "NOT " if kind == "not_exists" else ""
    body = "SELECT r0." + outer_col + " AS c0 FROM {" + outer + "} r0 WHERE " + negate
    body += "EXISTS (SELECT 1 FROM {" + inner + "} r1 WHERE r1." + inner_col
    body += " = r0." + outer_col + ")"
    return GenQuery(body, tables, "subquery")


def _scalar_subquery(outer, outer_col, inner, inner_col, tables):
    """Render a correlated scalar-subquery projection over an edge."""
    body = "SELECT r0." + outer_col + " AS c0, (SELECT COUNT(*) FROM {" + inner + "} r1"
    body += " WHERE r1." + inner_col + " = r0." + outer_col + ") AS c1"
    body += " FROM {" + outer + "} r0"
    return GenQuery(body, tables, "subquery")


@st.composite
def window_query(draw):
    """Generate a window function over a single table with a total-order tiebreak.

    The window ORDER BY always ends in a unique key, so ROW_NUMBER/running sums
    are deterministic per row and the multiset comparison is stable.
    """
    name = draw(st.sampled_from(sorted(_WINDOW_TABLES)))
    unique_col, order_col, part_col = _WINDOW_TABLES[name]
    func = draw(
        st.sampled_from(
            ("ROW_NUMBER()", "RANK()", "DENSE_RANK()", "SUM(r0." + order_col + ")")
        )
    )
    over = (
        "OVER (PARTITION BY r0."
        + part_col
        + " ORDER BY r0."
        + order_col
        + ", r0."
        + unique_col
        + ")"
    )
    sql = "SELECT r0." + unique_col + " AS c0, " + func + " " + over + " AS c1"
    sql += " FROM {" + name + "} r0"
    return GenQuery(sql, [name], "window")


# Per-table (unique key, order column, partition column) for window shapes.
_WINDOW_TABLES = {
    "orders": ("order_id", "price", "customer_id"),
    "fact_sales": ("sale_id", "amount", "item_key"),
    "products": ("product_id", "unit_price", "category"),
}


# Per-table unique key, giving an ORDER BY a deterministic total order so an
# order-sensitive result compares position by position between engine and oracle.
_UNIQUE_KEYS = {
    "orders": "order_id",
    "products": "product_id",
    "customers": "customer_id",
    "fact_sales": "sale_id",
    "dim_day": "day_key",
    "dim_item": "item_key",
    "t_null_a": "id",
    "t_null_b": "id",
    "t_dates": "d_id",
    "t_text": "s_id",
    "t_lookup": "code",
}


@st.composite
def order_query(draw):
    """Generate an ORDER BY (optionally LIMIT) over a unique key: order-checked.

    Ordering by a unique key is a total order, so both engine and oracle emit the
    rows in the same sequence and the result is compared position by position.
    """
    name = draw(st.sampled_from(sorted(_UNIQUE_KEYS)))
    aliases = _aliases_for([name])
    key = _named(name, _UNIQUE_KEYS[name])
    proj = draw(_projection(_choice_columns([name], aliases), draw(st.integers(1, 3))))
    key_ref = _colref(aliases[name], key)
    sql = "SELECT " + proj + " FROM " + _placeholder(name, aliases[name])
    sql += " ORDER BY " + key_ref + draw(_maybe_limit())
    return GenQuery(sql, [name], "order", order_sensitive=True)


@st.composite
def _maybe_limit(draw):
    """Draw an optional ``LIMIT`` clause (safe under a total order)."""
    if not draw(st.booleans()):
        return ""
    return " LIMIT " + str(draw(st.sampled_from((1, 2, 3, 5))))


@st.composite
def cte_query(draw):
    """Wrap a base query in a CTE that a second reference consumes (join reuse)."""
    edge = draw(st.sampled_from(JOIN_EDGES[:4]))
    outer, outer_col, inner, inner_col = edge
    tables = sorted({outer, inner})
    cte = "WITH base AS (SELECT " + outer_col + " AS k FROM {" + outer + "}) "
    body = cte + "SELECT a.k AS c0 FROM base a JOIN base b ON a.k = b.k"
    return GenQuery(body, tables, "cte")


@st.composite
def derived_query(draw):
    """Wrap a filtered single-table select in a derived table."""
    inner = draw(filter_query())
    body = "SELECT sub.c0 AS c0 FROM (" + inner.sql + ") sub"
    return GenQuery(body, inner.tables, "derived")


_SHAPES = (
    (join_query, 5),
    (aggregate_query, 4),
    (subquery_query, 4),
    (filter_query, 3),
    (setop_query, 2),
    (window_query, 2),
    (order_query, 2),
    (cte_query, 2),
    (derived_query, 2),
    (project_query, 1),
)


def queries():
    """The top-level strategy: a federated-biased weighted mix of every shape."""
    branches = []
    weights = []
    for builder, weight in _SHAPES:
        branches.append(builder())
        weights.append(weight)
    return _weighted(branches, weights)


def _weighted(branches, weights):
    """Return a strategy choosing among branches by integer weight."""
    pairs = []
    for branch, weight in zip(branches, weights):
        pairs.append((weight, branch))
    return st.one_of(_expand_weighted(pairs))


def _expand_weighted(pairs):
    """Expand (weight, branch) pairs into a flat list repeating by weight."""
    expanded = []
    for weight, branch in pairs:
        for _index in range(weight):
            expanded.append(branch)
    return expanded
