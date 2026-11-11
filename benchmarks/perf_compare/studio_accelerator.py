"""Run the complete three-source accelerator lifecycle through compare.py."""

import argparse
from contextlib import ExitStack
import json
import re
import subprocess
import sys

from studio_inputs import (
    STUDIO,
    VIEW,
    drop_owned_view,
    expected_results,
    require_clean,
    scenario_config,
    source_sql,
    upstream,
    verify_restoration,
)
from studio_measure import archive_chunks, capture, measure_phase
from studio_report import metadata, save_report, summaries
from wire_runtime import COUNTS, ROOT, cold_worker, execute, server


def arguments(argv):
    """Select the report and sample counts without omitting lifecycle phases."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--config", default=str(STUDIO / "generated/fedq.yaml"))
    parser.add_argument("--cold-runs", type=int, default=3)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=2)
    args = parser.parse_args(argv)
    validate_arguments(parser, args)
    return args


def validate_arguments(parser, args):
    """Require repeated cold/warm evidence and a filename-safe unique report label."""
    for name in ("cold_runs", "runs"):
        if getattr(args, name) < 3:
            parser.error(f"--{name.replace('_', '-')} must be at least 3")
    validate_label(parser, args)


def validate_label(parser, args):
    """Keep output under results and require at least one untimed warmup."""
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.label) is None:
        parser.error("--label must be a simple filename component")
    if args.warmups < 1:
        parser.error("--warmups must be positive")


def statement_count(args):
    """Count targets, setup, inspection, lifecycle preparation, and cleanup requests."""
    targets = 7 * (args.cold_runs + args.warmups + args.runs)
    setup = 3 * (1 + 7 * args.cold_runs)
    drops = args.cold_runs + args.warmups + args.runs - 1
    return targets + setup + drops + 30


def announce(args, output):
    """Print the complete workload before building or measuring anything."""
    timed = 7 * (args.cold_runs + args.runs)
    warmups = 7 * args.warmups
    print(
        f"{statement_count(args)} FedQ client statements: {timed} timed targets, "
        f"{warmups} untimed warmups, plus setup/inspection/cleanup.",
        flush=True,
    )
    print(
        "Two upstream mutation statements (insert and cleanup). All seven phases run.",
        flush=True,
    )
    print(f"Complete results and server/worker logs: {output}", flush=True)


def read_phase(name, query, expected, substitution=True):
    """Describe one expected result and whether automatic substitution is enabled."""
    return {
        "name": name,
        "sql": query,
        "expected": expected,
        "substitution": substitution,
    }


def ddl_phase(name, query, prefix):
    """Describe one CREATE or full-refresh target with its required status."""
    return {"name": name, "sql": query, "expected_status": prefix, "substitution": True}


def before_phases(report):
    """Measure the live join, full view build, and subsequent substituted reads."""
    query = report["query"]
    expected = report["oracle"]["before"]
    build = f"CREATE MATERIALIZED VIEW {VIEW} AS {query}"
    return [
        read_phase("live", query, expected),
        ddl_phase("build", build, "CREATE MATERIALIZED VIEW"),
        read_phase("cached", query, expected),
    ]


def after_phases(report):
    """Expose stale reads, verify live new data, refresh, and read the new cache."""
    query = report["query"]
    refresh = f"REFRESH MATERIALIZED VIEW {VIEW}"
    return [
        read_phase("stale", query, report["oracle"]["before"]),
        read_phase("bypass", query, report["oracle"]["after"], False),
        ddl_phase("refresh", refresh, "REFRESH MATERIALIZED VIEW (whole re-pull:"),
        read_phase("refreshed", query, report["oracle"]["after"]),
    ]


def configure_phase(client, phase):
    """Change the parent connection only at the explicit bypass/refresh boundaries."""
    if phase["name"] in ("bypass", "refresh"):
        value = str(phase["substitution"]).lower()
        execute(client, f"SET accelerator.enable_substitution = {value}")


def capture_refresh(client, report):
    """Record DDL metadata without mislabeling the SELECT-only fragment journal."""
    report["refresh"] = {
        "previous_select_fragments_after_refresh_ddl": execute(
            client, "SHOW PUSHDOWN FRAGMENTS"
        ),
        "views": execute(client, "SHOW MATERIALIZED VIEWS"),
        "note": "CREATE/REFRESH do not replace the SELECT fragment journal; "
        "inspect the actual SQL trace in live.server.log.",
    }


def inspect_phase(output, client, report, phase):
    """Keep proof of real scans, cache reads, persisted bytes, and refresh semantics."""
    name = phase["name"]
    if name == "refresh":
        capture_refresh(client, report)
        return
    if "expected_status" in phase:
        return
    expected = phase["expected"]
    report["inspections"][name] = capture(client, report["query"], name, expected)
    if name in ("cached", "stale", "refreshed"):
        report["chunks"][name] = archive_chunks(output, name)


def run_phases(args, output, config, client, report, phases):
    """Measure and inspect every phase in order on the parent's live runtime."""
    for phase in phases:
        with (output / "live.server.log").open("a") as log:
            log.write(f"\n[harness] phase={phase['name']}\n")
        configure_phase(client, phase)
        measure_phase(args, output, config, client, report, phase)
        inspect_phase(output, client, report, phase)


def experiment(args, output, config, report):
    """Mutate actual MySQL data and always restore only the experiment's artifacts."""
    with server(config, output / "live.server.log") as client:
        require_clean(client)
        with ExitStack() as cleanup:
            cleanup.callback(drop_owned_view, client)
            run_phases(args, output, config, client, report, before_phases(report))
            cleanup.callback(upstream, "accelerator-undo.sql")
            upstream("accelerator-insert.sql")
            run_phases(args, output, config, client, report, after_phases(report))
        verify_restoration(client, report["query"], report["oracle"]["before"])
        report["restored"] = True


def verify_counts(args, report):
    """Refuse completion if any planned engine statement was omitted."""
    actual = COUNTS["statements"]
    for sample in report["samples"]:
        actual += sample.get("statements", 0)
    report["executed_statements"] = actual
    if actual != statement_count(args):
        raise ValueError(
            f"Expected {statement_count(args)} statements, executed {actual}"
        )


def verify_chunks(report):
    """Require unchanged stored content before refresh and changed content afterward."""
    hashes = {}
    for phase, chunks in report["chunks"].items():
        hashes[phase] = chunk_hashes(chunks)
    if hashes["cached"] != hashes["stale"]:
        raise ValueError("Source insertion unexpectedly changed cached chunks")
    if hashes["cached"] == hashes["refreshed"]:
        raise ValueError("Refresh did not change the stored result content")


def chunk_hashes(chunks):
    """Extract ordered content hashes without discarding any persisted chunk."""
    result = []
    for chunk in chunks:
        result.append(chunk["sha256"])
    return result


def initial_report(args, query):
    """Create the complete evidence record before any source mutation occurs."""
    return {
        "meta": metadata(args),
        "query": query,
        "oracle": expected_results(query),
        "samples": [],
        "inspections": {},
        "chunks": {},
        "completed": False,
    }


def run(args, output):
    """Save complete or partial evidence, including failures and cleanup outcomes."""
    config = scenario_config(args.config, output)
    query = source_sql("queries/06_accelerator_support.sql")
    report = initial_report(args, query)
    try:
        experiment(args, output, config, report)
        verify_counts(args, report)
        verify_chunks(report)
        report["summary"] = summaries(report)
        report["completed"] = True
    finally:
        save_report(output, report)
    print((output / "REPORT.md").read_text(), flush=True)


def main(argv):
    """Dispatch a cold worker or the entire repeatable accelerator experiment."""
    if argv == ["--worker"]:
        cold_worker(json.load(sys.stdin))
        return
    args = arguments(argv)
    output = ROOT / "benchmarks" / "perf_compare" / "results" / args.label
    output.mkdir(parents=True, exist_ok=False)
    announce(args, output)
    subprocess.run(["make", "build"], cwd=ROOT, check=True)
    run(args, output)
