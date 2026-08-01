"""Run every corpus variation in its own process, against its own fresh engine.

One variation, one process, one engine that has executed nothing else. That is
the only way to learn what the engine does with a query, rather than what it
does with a query after eighty thousand others have trained it.

Why a whole process and not just a new Runtime: the engine's connector registry
is shared by every runtime inside a process, so a second Runtime in the same
process does not start clean. Each variation therefore runs in a child of its
own that builds its own Runtime over its own config, runs one query, and exits.

Every outcome is pass or fail. A query killed by the planning budget is a
FAILURE like any other - the budget is wall clock and the engine is expected to
plan inside it from cold, so a cold plan that blows it is the engine failing.
The failing error text is recorded, so budget kills are visible as a group
rather than excused as one.

Workers run in parallel when asked. Each process gets its OWN copy of the
harness: the engine opens DuckDB files read-write and DuckDB takes an exclusive
lock on a database file, so two processes cannot read the same one. The copy is
a copy-on-write clone (cp --reflink), which costs about 8ms for the whole 28MB
harness against a runtime build of about 1100ms, so per-process data isolation
is effectively free. The private config that points at it also gives the process
a private learned-statistics catalog, since the engine keeps that beside the
config it was launched from. PostgreSQL schemas stay shared: they are read
concurrently without conflict.

The planning budget is wall clock and covers PLANNING only, not execution. On
this harness the data is tiny and a query is dominated by connection setup
rather than CPU, so parallel workers are not expected to starve each other's
planning. If they ever do, the resulting kills are recorded as the failures they
are, with their error text, rather than excused.

Results are appended to the output file as they happen, one JSON object per
line, so a run that dies has lost nothing and can be resumed.
"""

import json
import multiprocessing
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from tests.e2e_federated import harness  # noqa: E402
from tests.e2e_federated.cases import all_cases, case_table_specs  # noqa: E402

DEFAULT_OUTPUT = "isolated-results.jsonl"

# Per-process harness clones live outside the harness itself, on the same
# filesystem so the clone can be copy-on-write.
WORKER_ROOT = harness.HARNESS_ROOT + "-workers"


def work_items():
    """Return every (case, table->source assignment) the corpus defines."""
    items = []
    for case in all_cases():
        specs = case_table_specs(case)
        for variation in harness.variations(specs, case.get("min_sources", 1)):
            items.append((case, variation.assignment))
    return items


def run_one(item):
    """Run one variation in this (fresh) process and return its result dict.

    Called as a pool task with one task per process, so everything this touches
    dies with it.
    """
    case, assignment = item
    variation = harness.Variation(assignment)
    root, config = _private_harness()
    started = time.time()
    try:
        outcome, detail = _execute(case, variation, config)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return {
        "case": case["name"],
        "variation": variation.name,
        "outcome": outcome,
        "detail": detail,
        "seconds": round(time.time() - started, 3),
    }


def _private_harness():
    """Clone the harness for this process and return (root, config path).

    The DuckDB files are cloned because the engine opens them read-write and
    DuckDB locks a database file exclusively, so processes cannot share one. The
    clone is copy-on-write, so it costs almost nothing and almost no disk. The
    config written beside it also gives this process its own, empty
    learned-statistics catalog. PostgreSQL schemas are read-only here and stay
    shared.
    """
    root = os.path.join(WORKER_ROOT, "w%d" % os.getpid())
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    for part in ("duck", "parquet"):
        subprocess.run(
            ["cp", "--reflink=auto", "-r",
             os.path.join(harness.HARNESS_ROOT, part), os.path.join(root, part)],
            check=True,
        )
    config = os.path.join(root, "config.yaml")
    handle = open(config, "w")
    handle.write(_rewritten_config(root))
    handle.close()
    return root, config


def _rewritten_config(root):
    """Return the config text with datasource paths pointed at a private clone."""
    handle = open(harness.CONFIG_PATH)
    text = handle.read()
    handle.close()
    for part in ("duck", "parquet"):
        text = text.replace(
            os.path.join(harness.HARNESS_ROOT, part), os.path.join(root, part)
        )
    return text


def _execute(case, variation, config):
    """Return (outcome, detail) for one variation against a brand new engine."""
    from tests.e2e_federated import runtime as runtime_mod

    engine = runtime_mod.Engine(config)
    environment = runtime_mod.open_variation(engine, variation)
    if "expect_error" in case:
        return _check_raises(environment, case)
    return _check_result(environment, case, variation)


def _check_raises(environment, case):
    """Return the outcome for a case that is required to raise."""
    try:
        environment.execute(case["query"])
    except Exception as error:
        if case["expect_error"] in str(error):
            return "pass", ""
        return "fail", "wrong error: " + _first_line(error)
    return "fail", "did not raise; expected " + case["expect_error"]


def _check_result(environment, case, variation):
    """Return the outcome for a case compared against the oracle."""
    from tests.e2e_federated.compare import compare_tables
    from tests.e2e_federated.oracle import build_oracle

    try:
        engine_table = environment.execute(case["query"])
    except Exception as error:
        return "fail", "raised: " + _first_line(error)
    reference = build_oracle(case_table_specs(case)).run(case["query"])
    try:
        compare_tables(engine_table, reference, variation.name,
                       case.get("order_sensitive", False))
    except AssertionError as error:
        return "fail", "mismatch: " + _first_line(error)
    return "pass", ""


def _first_line(error):
    """Return the first line of an exception's message, trimmed."""
    return str(error).splitlines()[0][:160]


def already_done(path):
    """Return the set of (case, variation) already recorded in the output file."""
    done = set()
    if not os.path.exists(path):
        return done
    handle = open(path)
    for line in handle:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        done.add((record["case"], record["variation"]))
    handle.close()
    return done


def pending(items, done):
    """Return the work items not already recorded in the output file."""
    remaining = []
    for case, assignment in items:
        key = (case["name"], harness.Variation(assignment).name)
        if key not in done:
            remaining.append((case, assignment))
    return remaining


def _report(handle, record, counts, index, total, started):
    """Append one result and print progress; return the running failure count."""
    handle.write(json.dumps(record) + "\n")
    handle.flush()
    counts[record["outcome"]] = counts.get(record["outcome"], 0) + 1
    if index % 200 and index != total:
        return
    elapsed = time.time() - started
    rate = index / elapsed if elapsed else 0
    left = (total - index) / rate / 60 if rate else 0
    sys.stdout.write(
        "  %d/%d  pass=%d fail=%d  %.1f/s  ~%.0f min left\n"
        % (index, total, counts.get("pass", 0), counts.get("fail", 0), rate, left)
    )
    sys.stdout.flush()


def main():
    """Run every pending variation in its own process, recording each result."""
    output = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    items = work_items()
    remaining = pending(items, already_done(output))
    total = len(remaining)
    sys.stdout.write(
        "%d variations total, %d already recorded, %d to run on %d worker(s) -> %s\n"
        % (len(items), len(items) - total, total, workers, output)
    )
    sys.stdout.flush()
    shutil.rmtree(WORKER_ROOT, ignore_errors=True)
    os.makedirs(WORKER_ROOT, exist_ok=True)
    handle = open(output, "a")
    counts = {}
    started = time.time()
    # One task per process: every variation gets an engine that has run nothing.
    pool = multiprocessing.Pool(processes=workers, maxtasksperchild=1)
    index = 0
    for record in pool.imap(run_one, remaining):
        index += 1
        _report(handle, record, counts, index, total, started)
    pool.close()
    pool.join()
    handle.close()
    sys.stdout.write(
        "DONE  pass=%d fail=%d in %.1f min\n"
        % (counts.get("pass", 0), counts.get("fail", 0),
           (time.time() - started) / 60)
    )


if __name__ == "__main__":
    main()
