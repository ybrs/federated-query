"""Dump the NEW (Rust) engine's PHYSICAL plan per query via EXPLAIN.

Pure planning: parse -> bind -> decorrelate -> optimize -> physical, then render
the plan tree. NO execution, so no data is touched and each query is sub-second.
A per-query SIGALRM hard-kills any plan that runs away (should never fire).

Usage: python dump_new_explain.py <tpch|tpcds>
Writes benchmarks/plan_compare/new/<suite>/<q>.txt and prints per-query timing.
"""

import glob
import os
import signal
import sys
import time

from dump_new import (
    _bench_dir,
    _write_config,
    _adbc_driver_path,
    _make_qualifier,
    SUITE_SCALE,
    SUITE_PG_DB,
    HERE,
)

import pyarrow as pa

HARD_KILL_SECONDS = 15  # hang guard; real planning is sub-second


class _Timeout(Exception):
    pass


def _explain_text(runtime, engine_sql):
    """Return the new engine's EXPLAIN plan lines for one query (or raise)."""
    result = runtime.execute("EXPLAIN " + engine_sql)
    table = pa.table(result)
    lines = []
    for row in table.to_pylist():
        lines.append(list(row.values())[0])
    return lines


def _dump_one(runtime, qualify, path, out_dir):
    """Plan one query, write its EXPLAIN tree, return (name, seconds, status)."""
    name = os.path.splitext(os.path.basename(path))[0]
    engine_sql = qualify(open(path).read())
    signal.alarm(HARD_KILL_SECONDS)
    start = time.time()
    error = None
    lines = []
    try:
        lines = _explain_text(runtime, engine_sql)
    except _Timeout:
        error = "TIMEOUT (killed at {0}s)".format(HARD_KILL_SECONDS)
    except BaseException as caught:
        detail = str(caught).strip().splitlines()
        error = "{0}: {1}".format(type(caught).__name__, detail[0] if detail else "")
    finally:
        signal.alarm(0)
    seconds = time.time() - start
    body = ["QUERY: {0}".format(name), "ENGINE: new (rust) EXPLAIN", ""]
    if error:
        body.append("ERROR: {0}".format(error))
    else:
        body.extend(lines)
    with open(os.path.join(out_dir, name + ".txt"), "w") as handle:
        handle.write("\n".join(body))
    return name, seconds, ("ERROR: " + error if error else "ok {0} lines".format(len(lines)))


def _on_alarm(signum, frame):
    """SIGALRM handler: turn a runaway plan into a catchable timeout."""
    raise _Timeout()


def main():
    """Dump every query in a suite through the new engine's EXPLAIN, timed."""
    suite = sys.argv[1]
    signal.signal(signal.SIGALRM, _on_alarm)
    bench = _bench_dir(suite)
    sys.path.insert(0, bench)
    import fedq
    from generate import _db_path, DEFAULT_DATA_DIR

    db_path = _db_path(DEFAULT_DATA_DIR, SUITE_SCALE[suite])
    config_path = _write_config(db_path, SUITE_PG_DB[suite], _adbc_driver_path())
    runtime = fedq.Runtime(config_path)
    qualify = _make_qualifier(suite)
    out_dir = os.path.join(HERE, "new", suite)
    os.makedirs(out_dir, exist_ok=True)
    paths = sorted(glob.glob(os.path.join(bench, "queries", "q*.sql")))

    total = 0.0
    slow = 0
    errors = 0
    for path in paths:
        name, seconds, status = _dump_one(runtime, qualify, path, out_dir)
        total += seconds
        flag = "  <== SLOW" if seconds > 1.0 else ""
        if seconds > 1.0:
            slow += 1
        if status.startswith("ERROR"):
            errors += 1
        print("{0:5} {1:7.3f}s  {2}{3}".format(name, seconds, status, flag), flush=True)
    print(
        "\n[{0}] {1} queries, {2:.2f}s total, {3:.3f}s avg, {4} slow(>1s), {5} errors".format(
            suite, len(paths), total, total / max(len(paths), 1), slow, errors
        )
    )


if __name__ == "__main__":
    main()
