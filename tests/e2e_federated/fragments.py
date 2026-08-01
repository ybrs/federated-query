"""Verify every query the engine hands to a source against the DuckDB oracle.

A pushed fragment is where the engine stops executing and trusts a foreign
executor: the subtree is re-rendered as text, sent, and whatever comes back is
used. This module replays each distinct fragment two ways - the dialect-neutral
canonical form on the all-tables DuckDB oracle, and the fragment itself back
through the engine against its own source - and compares them.

Fragments are deduplicated by (source kind, canonical SQL), because the same
co-located group emits the same text under many distributions. That is what makes
this affordable where running every plan of every distribution is not.

A fragment carrying a dynamic filter received an IN-list of join keys appended at
execution time, so its recorded text is the base query rather than the exact one
the source ran. Replaying that base text on BOTH sides still compares like with
like, so these fragments are verified, not skipped - but the injected key filter
itself is outside what this check covers, and the count of such fragments is
reported so that limit is a number rather than an assumption.
"""

import datetime
import re
from decimal import Decimal

import pyarrow as pa

from tests.e2e_federated import harness
from tests.e2e_federated import known_fragment_divergences

# A quoted two-part reference: `"a"."b"`. The emitter writes both table
# references and column references this way, so a match is only a TABLE reference
# when its first part is one of the harness's schema names - an alias would have
# to be literally named `main` or `fed_pgN` to collide.
_QUOTED_PAIR = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"\."([A-Za-z_][A-Za-z0-9_]*)"')

# The relative tolerance two numerics compare within, matching `compare.py` so a
# fragment and a whole-query result are judged by the same rule.
_FLOAT_TOLERANCE = 1e-9

# Every schema a harness datasource exposes: DuckDB and Parquet under `main`,
# each PostgreSQL instance under its own `fed_pgN`.
_SCHEMAS = frozenset(
    [harness.DUCK_SCHEMA, harness.PARQUET_SCHEMA]
    + [harness.pg_schema(index) for index in range(1, harness.INSTANCES_PER_KIND + 1)]
)


def drain(runtime):
    """Return the fragments the last statement pushed, as a list of dicts."""
    return pa.table(runtime.execute("SHOW PUSHDOWN FRAGMENTS")).to_pylist()


def source_kind(datasource_name):
    """Return the harness source kind ('duck', 'pq' or 'pg') of a datasource."""
    for prefix in ("duck", "pq", "pg"):
        if datasource_name.startswith(prefix):
            return prefix
    raise RuntimeError("datasource is not a harness instance: " + datasource_name)


def oracle_sql(canonical_sql):
    """Rewrite a fragment's table references to the oracle's bare `main` schema.

    Every harness instance carries every table, and the oracle is one DuckDB
    holding all of them under `main`, so only the schema qualifier differs.
    """
    def rewrite(match):
        """Requalify one table reference to `main`, leaving columns alone."""
        if match.group(1) not in _SCHEMAS:
            return match.group(0)
        return '"main"."' + match.group(2) + '"'

    return _QUOTED_PAIR.sub(rewrite, canonical_sql)


def engine_sql(canonical_sql, datasource):
    """Rewrite a fragment's table references to name its datasource explicitly.

    The result is a valid federated query over exactly the tables the fragment
    read, so the engine plans it as a single-source push back to the same source.
    """
    def rewrite(match):
        """Prefix one table reference with its datasource, leaving columns alone."""
        if match.group(1) not in _SCHEMAS:
            return match.group(0)
        return '"' + datasource + '"."' + match.group(1) + '"."' + match.group(2) + '"'

    return _QUOTED_PAIR.sub(rewrite, canonical_sql)


class FragmentRecord:
    """One distinct fragment, and how many times the corpus produced it."""

    def __init__(self, datasource, canonical_sql, source_sql, output_names,
                 has_dynamic_filter):
        """Store the fragment's fields and start its occurrence count at one."""
        self.datasource = datasource
        self.canonical_sql = canonical_sql
        self.source_sql = source_sql
        self.output_names = output_names
        self.has_dynamic_filter = has_dynamic_filter
        self.occurrences = 1

    @property
    def key(self):
        """The identity two fragments share when they are the same check."""
        return (source_kind(self.datasource), self.canonical_sql)


class FragmentCollector:
    """The distinct fragments seen across a run, in first-seen order."""

    def __init__(self):
        """Start with no fragments recorded."""
        self.records = {}

    def add_from(self, runtime):
        """Fold the last statement's fragments into the distinct set."""
        for row in drain(runtime):
            self.add(row)

    def add(self, row):
        """Fold one fragment row in; return the record when it is newly seen.

        Returning only NEW records is what lets a caller verify each distinct
        fragment exactly once, however many times the corpus emits it.
        """
        record = FragmentRecord(
            row["datasource"],
            row["canonical_sql"],
            row["source_sql"],
            row["output_names"],
            row["has_dynamic_filter"],
        )
        existing = self.records.get(record.key)
        if existing is not None:
            existing.occurrences += 1
            return None
        self.records[record.key] = record
        return record

    def distinct(self):
        """Return the distinct fragments, in first-seen order."""
        return list(self.records.values())

    def occurrences(self):
        """Return how many fragment emissions the distinct set stands for."""
        total = 0
        for record in self.records.values():
            total += record.occurrences
        return total


class FragmentVerdict:
    """The outcome of checking one fragment against the oracle."""

    def __init__(self, record, failures, type_divergences, replay_sql):
        """Store the fragment and what the comparison found."""
        self.record = record
        self.failures = failures
        self.type_divergences = type_divergences
        self.replay_sql = replay_sql

    @property
    def failed(self):
        """Whether this fragment disagreed with the oracle on rows or names."""
        return bool(self.failures)

    @property
    def replayed_verbatim(self):
        """Whether the replay pushed exactly the fragment being verified.

        When it did, the rows compared are certainly this fragment's. When it did
        not, the engine re-planned an equivalent query - the row comparison still
        holds, but it covers the re-planned form rather than the recorded text, so
        the difference is reported.
        """
        return self.replay_sql == [self.record.source_sql]


def verify(record, runtime, oracle_connection):
    """Check one fragment against the oracle and return its verdict.

    The fragment is run two ways over the same data: its canonical form on the
    oracle, and the same query pushed back to its own source through the engine.
    Rows and column names must agree - that is the check. A type or precision
    difference is recorded separately, because which arithmetic convention the
    engine adopts is an open design decision rather than a defect in this
    fragment. The SQL the replay pushed is recorded too: when it is not the
    recorded text verbatim the engine re-planned an equivalent query, which the
    row comparison still covers but the verdict reports.
    """
    reference = oracle_sql(record.canonical_sql)
    expected = oracle_connection.execute(reference).to_arrow_table()
    replay = engine_sql(record.canonical_sql, record.datasource)
    actual = pa.table(runtime.execute(replay))
    pushed = []
    for row in drain(runtime):
        pushed.append(row["source_sql"])
    failures = _compare(expected, actual)
    divergences = _type_divergences(expected, actual)
    return FragmentVerdict(record, failures, divergences, pushed)


def _compare(expected, actual):
    """Return every way the replayed fragment disagreed with the oracle."""
    failures = []
    _check_column_names(expected, actual, failures)
    _check_rows(expected, actual, failures)
    return failures


def _check_column_names(expected, actual, failures):
    """Record a failure when the fragment's columns differ from the oracle's."""
    expected_names = _lower_names(expected)
    actual_names = _lower_names(actual)
    if expected_names != actual_names:
        failures.append(
            "column mismatch: source " + str(actual_names)
            + " vs oracle " + str(expected_names)
        )


def _check_rows(expected, actual, failures):
    """Record a failure when the fragment's rows differ from the oracle's."""
    expected_rows = _sorted_rows(expected)
    actual_rows = _sorted_rows(actual)
    if len(expected_rows) != len(actual_rows):
        failures.append(
            "row count: source " + str(len(actual_rows))
            + " vs oracle " + str(len(expected_rows))
        )
        return
    for index, expected_row in enumerate(expected_rows):
        if not _rows_equal(expected_row, actual_rows[index]):
            failures.append(
                "row " + str(index) + " differs: source " + repr(actual_rows[index])
                + " vs oracle " + repr(expected_row)
            )
            return


def _type_divergences(expected, actual):
    """Return the columns whose Arrow type differs between source and oracle."""
    divergences = []
    if _lower_names(expected) != _lower_names(actual):
        return divergences
    for index, field in enumerate(expected.schema):
        actual_type = actual.schema.field(index).type
        if field.type != actual_type:
            divergences.append(field.name + ": source " + str(actual_type)
                               + " vs oracle " + str(field.type))
    return divergences


def _lower_names(table):
    """Return a table's column names lower-cased, in order."""
    names = []
    for name in table.column_names:
        names.append(name.lower())
    return names


def _sorted_rows(table):
    """Return a table's rows as value tuples under one total order."""
    rows = []
    for row in table.to_pylist():
        values = []
        for name in table.column_names:
            values.append(row[name])
        rows.append(tuple(values))
    rows.sort(key=_row_sort_key)
    return rows


def _row_sort_key(row):
    """Return a None-safe (rank, number, text) key ordering any row."""
    keys = []
    for value in row:
        keys.append(_value_sort_key(value))
    return tuple(keys)


def _value_sort_key(value):
    """Return a None-safe sort key for one cell."""
    if value is None:
        return (0, 0.0, "")
    if isinstance(value, bool):
        return (1, float(value), "")
    if isinstance(value, (int, float, Decimal)):
        return (1, round(float(value), 6), "")
    if isinstance(value, (datetime.date, datetime.datetime)):
        return (3, 0.0, value.isoformat())
    return (2, 0.0, str(value))


def _rows_equal(expected_row, actual_row):
    """Whether two rows agree cell by cell under the suite's numeric tolerance."""
    for index, expected_value in enumerate(expected_row):
        if not _values_equal(expected_value, actual_row[index]):
            return False
    return True


def _values_equal(left, right):
    """Whether two cells are equal, comparing numerics across types."""
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if _is_number(left) and _is_number(right):
        return _floats_close(float(left), float(right))
    return left == right


def _is_number(value):
    """Whether a value is a non-boolean numeric."""
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _floats_close(left, right):
    """Whether two numbers are within the suite's relative tolerance."""
    scale = max(1.0, abs(left), abs(right))
    return abs(left - right) <= _FLOAT_TOLERANCE * scale


class FragmentVerifier:
    """Checks each distinct pushed fragment the first time the corpus emits it.

    Verifying on first sight is what keeps this affordable: the corpus emits a
    fragment many times over but each distinct one is checked once, so the cost is
    the number of distinct fragments rather than the number of variations. It also
    attaches a failure to the variation that first produced it, which is where the
    plan that emitted it can be seen.
    """

    def __init__(self, runtime, oracle_connection):
        """Store the engine and oracle this verifier compares, and start empty."""
        self.runtime = runtime
        self.oracle_connection = oracle_connection
        self.collector = FragmentCollector()
        self.type_divergences = []
        self.replanned = 0
        self.known_divergences = 0

    def observe(self):
        """Verify every newly-seen fragment of the statement just executed.

        Raises AssertionError naming the fragment and the disagreement when a
        source does not answer its own pushed SQL the way the oracle does.
        """
        for record in self._new_records():
            self._verify_one(record)

    def _new_records(self):
        """Drain the journal and return the records not seen before.

        The whole statement is drained BEFORE any verification runs, because
        verifying executes queries of its own and replaces the journal.
        """
        fresh = []
        for row in drain(self.runtime):
            record = self.collector.add(row)
            if record is not None:
                fresh.append(record)
        return fresh

    def _verify_one(self, record):
        """Check one fragment and raise on an unregistered disagreement.

        A fragment registered as a known divergence is held STRICTLY: it must
        still disagree, so the entry deletes itself in the change that makes the
        source agree.
        """
        verdict = verify(record, self.runtime, self.oracle_connection)
        self.type_divergences.extend(verdict.type_divergences)
        if not verdict.replayed_verbatim:
            self.replanned += 1
        reason = known_fragment_divergences.lookup(
            source_kind(record.datasource), record.canonical_sql
        )
        if reason is None:
            self._require_agreement(record, verdict)
        else:
            self._require_known_divergence(record, verdict, reason)

    def _require_agreement(self, record, verdict):
        """Raise unless this fragment agrees with the oracle."""
        if verdict.failed:
            raise AssertionError(_failure_message(record, verdict))

    def _require_known_divergence(self, record, verdict, reason):
        """Raise unless this registered fragment still disagrees."""
        self.known_divergences += 1
        if verdict.failed:
            return
        raise AssertionError(
            "fragment is registered as a known divergence but now agrees with "
            "the oracle; delete its entry from known_fragment_divergences\n"
            "  datasource: " + record.datasource + "\n"
            "  canonical:  " + record.canonical_sql + "\n"
            "  recorded reason: " + reason
        )

    def summary(self):
        """Return the one-line count of what this verifier checked."""
        return (
            "pushdown fragments: %d distinct from %d emissions, "
            "%d type-divergent, %d replanned on replay, %d known divergences"
            % (
                len(self.collector.distinct()),
                self.collector.occurrences(),
                len(self.type_divergences),
                self.replanned,
                self.known_divergences,
            )
        )


def _failure_message(record, verdict):
    """Build the message naming the fragment and every disagreement found."""
    lines = [
        "pushed fragment disagrees with the DuckDB oracle",
        "  datasource: " + record.datasource,
        "  sent:       " + record.source_sql,
        "  canonical:  " + record.canonical_sql,
        "  plan expected columns: " + str(record.output_names),
    ]
    for failure in verdict.failures:
        lines.append("  -> " + failure)
    return "\n".join(lines)
