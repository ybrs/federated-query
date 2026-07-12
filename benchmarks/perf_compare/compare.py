"""THE perf comparison harness: OLD (Python) vs NEW (Rust) engine, cold + warm.

This is the ONLY sanctioned way to produce engine perf numbers (no ad-hoc
runs - see the benchmarking guardrails in CLAUDE.md). It measures, per source
(single-source DuckDB, single-source Parquet, optionally federated pg+duck)
and per engine:

  plan  cold   ONE planning pass, fresh process + fresh runtime
  total cold   ONE execute(), fresh process + fresh runtime
  plan  warm   median of N planning passes on a live runtime (after warmups)
  total warm   median of N execute() on a live runtime (after warmups)
  fetch warm   source_scan time inside the last warm execute (FEDQRS_PROFILE)

COLD means a fresh OS process AND a fresh runtime - a cached statistic or plan
cannot leak in. The NEW engine's 100ms planning budget applies: a query whose
planning blows it is reported as KILLED with the engine's own report. An engine
that cannot build a source (e.g. NEW has no parquet connector yet) is reported
as UNSUPPORTED. Nothing is skipped silently.

Usage:
  /workspace/venv-fedq/bin/python compare.py --scale-factor 1 --label sf1
  ... --sources duck,parquet --queries q01,q06,q12   (defaults: all 22)
  ... --planning-budget-ms 100000                    (lift the kill to measure)

Writes results/<label>/results.json and results/<label>/REPORT.md.
"""

import argparse
import glob
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
RELEASE_LIB = os.path.join(ROOT, "target", "release", "libfedq_py.so")


def _all_queries():
    """Every TPC-H query name (q01..q22) from the queries directory."""
    names = []
    for path in sorted(glob.glob(os.path.join(ROOT, "benchmarks", "tpch", "queries", "q*.sql"))):
        names.append(os.path.splitext(os.path.basename(path))[0])
    return names


def _build_release():
    """Build the RELEASE fedq-py and point the local fedq.so symlink at it.

    Release is mandatory for perf numbers: a debug build times the compiler's
    laziness, not the engine. DuckDB itself is prebuilt (never compiled here),
    so this is quick after the first workspace release build.
    """
    env = dict(os.environ)
    env.setdefault("CARGO_TARGET_DIR", os.path.join(ROOT, "target"))
    subprocess.run(
        ["cargo", "build", "--release", "-p", "fedq-py"], cwd=ROOT, env=env, check=True
    )
    link = os.path.join(HERE, "fedq.so")
    if os.path.islink(link) or os.path.exists(link):
        os.remove(link)
    os.symlink(RELEASE_LIB, link)


def _worker_cmd(engine, source, mode, queries, args):
    """The worker.py invocation for one (engine, source, mode) cell."""
    cmd = [
        sys.executable,
        os.path.join(HERE, "worker.py"),
        "--engine", engine,
        "--source", source,
        "--mode", mode,
        "--queries", ",".join(queries),
        "--scale-factor", args.scale_factor,
        "--pg-database", args.pg_database,
        "--warmups", str(args.warmups),
        "--runs", str(args.runs),
    ]
    if args.planning_budget_ms is not None:
        cmd += ["--planning-budget-ms", str(args.planning_budget_ms)]
    return cmd


def _run_worker(cmd):
    """Run one worker; return its JSON records. A crashed worker fails LOUD."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(
            "worker failed: {0}\nstdout:\n{1}\nstderr:\n{2}".format(
                " ".join(cmd), result.stdout, result.stderr[-4000:]
            )
        )
    records = []
    for line in result.stdout.splitlines():
        if line.strip():
            records.append(json.loads(line))
    return records


def _measure_cell(engine, source, queries, args):
    """All measurements for one (engine, source): cold per query + one warm."""
    records = []
    for query in queries:
        for mode in ["cold-plan", "cold-total"]:
            cmd = _worker_cmd(engine, source, mode, [query], args)
            records.extend(_run_worker(cmd))
            _progress(engine, source, mode, query, records[-1])
    warm = _run_worker(_worker_cmd(engine, source, "warm", queries, args))
    for record in warm:
        _progress(engine, source, "warm", record["query"], record)
    records.extend(warm)
    return records


def _progress(engine, source, mode, query, record):
    """One live progress line per finished measurement."""
    value = record.get("plan_ms", record.get("total_ms"))
    shown = "{0:.1f}ms".format(value) if value is not None else record["status"]
    print("  {0:3} {1:7} {2:10} {3:4} {4}".format(engine, source, mode, query, shown))


def _index(records):
    """(query, mode) -> record for one engine+source's records."""
    table = {}
    for record in records:
        table[(record["query"], record["mode"])] = record
    return table


def _cell_text(record, field):
    """One table cell: the metric, or the loud status when it is absent."""
    if record is None:
        return "-"
    if record["status"] != "ok":
        return record["status"].upper()
    value = record.get(field)
    return "-" if value is None else "{0:.1f}".format(value)


def _report_source(source, by_engine, queries, lines):
    """Append one source's comparison table to the report lines."""
    lines.append("")
    lines.append("## source: {0}".format(source))
    lines.append("")
    lines.append(
        "| query | plan cold old | plan cold new | plan warm old | plan warm new "
        "| total cold old | total cold new | total warm old | total warm new | new status |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for query in queries:
        lines.append(_report_row(query, by_engine))
    _report_summary(by_engine, queries, lines)


def _report_row(query, by_engine):
    """One query's row across both engines."""
    old = by_engine.get("old", {})
    new = by_engine.get("new", {})
    cells = [
        _cell_text(old.get((query, "cold-plan")), "plan_ms"),
        _cell_text(new.get((query, "cold-plan")), "plan_ms"),
        _cell_text(old.get((query, "warm")), "plan_ms"),
        _cell_text(new.get((query, "warm")), "plan_ms"),
        _cell_text(old.get((query, "cold-total")), "total_ms"),
        _cell_text(new.get((query, "cold-total")), "total_ms"),
        _cell_text(old.get((query, "warm")), "total_ms"),
        _cell_text(new.get((query, "warm")), "total_ms"),
    ]
    status = _new_status(new, query)
    return "| {0} | {1} | {2} |".format(query, " | ".join(cells), status)


def _new_status(new, query):
    """The NEW engine's worst status across a query's modes, with its message."""
    worst = "ok"
    message = ""
    for mode in ["cold-plan", "cold-total", "warm"]:
        record = new.get((query, mode))
        if record is not None and record["status"] != "ok":
            worst = record["status"].upper()
            message = record.get("message", "")
    if worst == "ok":
        return "ok"
    return "{0}: {1}".format(worst, message[:160])


def _report_summary(by_engine, queries, lines):
    """Per-source totals over queries BOTH engines completed, plus new-status counts."""
    old = by_engine.get("old", {})
    new = by_engine.get("new", {})
    sums = {"old": {"cold": 0.0, "warm": 0.0}, "new": {"cold": 0.0, "warm": 0.0}}
    compared = 0
    counts = {}
    for query in queries:
        status = _new_status(new, query).split(":")[0].lower()
        counts[status] = counts.get(status, 0) + 1
        rows = [old.get((query, "cold-total")), new.get((query, "cold-total")),
                old.get((query, "warm")), new.get((query, "warm"))]
        if any(r is None or r["status"] != "ok" for r in rows):
            continue
        compared += 1
        sums["old"]["cold"] += rows[0]["total_ms"]
        sums["new"]["cold"] += rows[1]["total_ms"]
        sums["old"]["warm"] += rows[2]["total_ms"]
        sums["new"]["warm"] += rows[3]["total_ms"]
    lines.append("")
    lines.append(
        "totals over the {0} queries both engines completed: "
        "cold old {1:.1f}ms / new {2:.1f}ms; warm old {3:.1f}ms / new {4:.1f}ms".format(
            compared, sums["old"]["cold"], sums["new"]["cold"],
            sums["old"]["warm"], sums["new"]["warm"],
        )
    )
    lines.append("new-engine statuses: {0}".format(json.dumps(counts, sort_keys=True)))


def _git_commit():
    """The current git commit id (for the report's reproducibility header)."""
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def main():
    """Run the full matrix and write results.json + REPORT.md."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale-factor", default="1")
    parser.add_argument("--label", default=None, help="results/<label>/ (default sf<sf>)")
    parser.add_argument("--sources", default="duck,parquet")
    parser.add_argument("--engines", default="old,new")
    parser.add_argument("--queries", default="all")
    parser.add_argument("--pg-database", default="duckpoc_sf1")
    parser.add_argument("--planning-budget-ms", type=int, default=None)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    if not args.skip_build:
        _build_release()
    queries = _all_queries() if args.queries == "all" else args.queries.split(",")
    sources = args.sources.split(",")
    engines = args.engines.split(",")

    results = {"meta": {"git_commit": _git_commit(), "args": vars(args)}, "cells": {}}
    report = {}
    for source in sources:
        report[source] = {}
        for engine in engines:
            print("== {0} engine on {1} (SF{2}) ==".format(engine, source, args.scale_factor))
            records = _measure_cell(engine, source, queries, args)
            results["cells"]["{0}/{1}".format(engine, source)] = records
            report[source][engine] = _index(records)

    label = args.label or "sf{0}".format(args.scale_factor)
    out_dir = os.path.join(HERE, "results", label)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "results.json"), "w") as handle:
        json.dump(results, handle, indent=1)
    lines = [
        "# Perf compare: OLD (python) vs NEW (rust) - {0}".format(label),
        "",
        "commit {0}; SF{1}; warmups {2}; runs {3} (medians); planning budget {4}; "
        "all values unrounded ms; COLD = fresh process + fresh runtime.".format(
            results["meta"]["git_commit"], args.scale_factor, args.warmups, args.runs,
            "engine default" if args.planning_budget_ms is None
            else "{0}ms".format(args.planning_budget_ms),
        ),
    ]
    for source in sources:
        _report_source(source, report[source], queries, lines)
    report_path = os.path.join(out_dir, "REPORT.md")
    with open(report_path, "w") as handle:
        handle.write("\n".join(lines) + "\n")
    print("wrote {0}".format(report_path))


if __name__ == "__main__":
    main()
