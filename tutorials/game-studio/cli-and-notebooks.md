# Use the same engine in a CLI or notebook

The showcase uses a server so clients can connect to a shared endpoint.
For a one-off query, the CLI runs the engine directly with no FedQ server
component. The three source databases still need to be running:

```bash
target/release/fedq \
  --config tutorials/game-studio/generated/fedq.yaml \
  --file tutorials/game-studio/queries/01_support_by_plan.sql
```

For a notebook or marimo notebook, the Python library also embeds the engine
without a FedQ server. The repository build produces the native extension;
on Linux, make it importable under its module name:

```bash
ln -s ../../../target/release/libfedq_py.so tutorials/game-studio/generated/fedq.so
export PYTHONPATH="$PWD/tutorials/game-studio/generated${PYTHONPATH:+:$PYTHONPATH}"
```

For marimo and charts, install the optional packages in the active environment
and launch marimo from this same terminal:

```bash
"$VIRTUAL_ENV/bin/python3" -m pip install marimo pandas matplotlib
"$VIRTUAL_ENV/bin/python3" -m marimo edit
```

Start the notebook from the repository root with the active virtualenv as its
Python environment. In a cell, execute SQL and get an Arrow table:

```python
from pathlib import Path

import fedq
import pyarrow as pa

runtime = fedq.Runtime("tutorials/game-studio/generated/fedq.yaml")
sql = Path("tutorials/game-studio/queries/01_support_by_plan.sql").read_text()
support = pa.table(runtime.execute(sql))
support
```

`sql` is the support query printed in full in [chapter 1](01-support-by-plan.md). With `pandas` and
`matplotlib` installed in that environment, another cell can draw it:

```python
frame = support.to_pandas()
chart = frame.plot.bar(
    x="subscription_plan",
    y="tickets_per_100_players",
    legend=False,
    ylabel="Tickets per 100 players",
    rot=0,
)
chart.figure
```

In marimo, put setup/querying in one cell and plotting in another. Returning
`chart.figure` as the final expression displays the figure. The returned
Arrow table can also feed other chart libraries; there is no server round
trip in this embedded-library path. To use the shared server from a Python
notebook instead, connect with a PostgreSQL driver such as `psycopg2` and
explicitly set `connection.autocommit = True` before executing queries.

---

[Contents](README.md) | [Previous: inspection-and-verification.md](inspection-and-verification.md) | [Next: reset-and-extend.md](reset-and-extend.md)
