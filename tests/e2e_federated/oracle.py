"""Ground-truth oracle: run a case query on a single all-tables DuckDB.

The oracle seeds every table a case uses into one in-memory DuckDB connection and
runs the case query there (placeholders filled with bare table names, since all
tables live in that database's ``main`` schema). Its result is the correctness
reference every placement's engine result is compared against. SELECT queries do
not mutate, so one connection serves repeated runs for a table set.
"""

import duckdb

from tests.e2e_federated import runtime as runtime_mod


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
    """Seed all of a case's tables into one DuckDB and return its Oracle."""
    connection = duckdb.connect()
    runtime_mod._run_specs(connection, _spec_list(specs))
    mapping = _bare_mapping(specs)
    return Oracle(connection, mapping)


def _spec_list(specs):
    """Return the TableSpecs of a name -> spec map as a list."""
    values = []
    for name in specs:
        values.append(specs[name])
    return values


def _bare_mapping(specs):
    """Return a table -> itself map: the oracle references tables by bare name."""
    mapping = {}
    for name in specs:
        mapping[name] = name
    return mapping
