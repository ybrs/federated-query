"""Ground-truth oracle: run a case query on a single all-tables DuckDB.

The oracle is one DuckDB file per distinct table set, carrying every table a case
uses, built by ``build_test_harness.sh``. The case query runs there with its
placeholders filled with bare table names, since all the tables live in that
database's ``main`` schema. Its result is the correctness reference every
placement's engine result is compared against.

Like every other source the suite reads, it is OPENED read-only and never
created here. The reference answer a test asserts against is therefore a file
that can be opened and inspected after a failure, not state that existed only
inside the run that failed.
"""

import os

import duckdb

from tests.e2e_federated import harness


class Oracle:
    """A seeded single-DuckDB reference: fills bare names and runs a query."""

    def __init__(self, connection, mapping):
        """Store the DuckDB connection and the table -> bare-name map."""
        self._connection = connection
        self._mapping = mapping

    def render(self, query):
        """Fill a case query's ``{table}`` placeholders with bare table names."""
        return query.format(**self._mapping)

    def run(self, query):
        """Run the rendered query and return its result as a pyarrow.Table."""
        rendered = self.render(query)
        return self._connection.execute(rendered).to_arrow_table()


def build_oracle(specs):
    """Open the ground-truth DuckDB and return its Oracle for a table set.

    Creates nothing. Every DuckDB instance already carries every table, so the
    first one IS the all-tables reference database; a missing one raises naming
    it and the command that builds it.
    """
    source = harness.oracle_source()
    if not os.path.exists(source.location):
        raise RuntimeError(
            "test harness is incomplete, oracle " + source.name + " is missing at "
            + source.location + "  --  build it with ./build_test_harness.sh"
        )
    connection = duckdb.connect(source.location, read_only=True)
    mapping = _bare_mapping(specs)
    return Oracle(connection, mapping)


def _bare_mapping(specs):
    """Return a table -> itself map: the oracle references tables by bare name."""
    mapping = {}
    for name in specs:
        mapping[name] = name
    return mapping
