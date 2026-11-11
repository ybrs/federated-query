"""Cold/warm wire measurements and physical-plan evidence for the accelerator."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys

from studio_inputs import DROP, VIEW, require_rows
from wire_runtime import ROOT, execute, measure


def require_success(record):
    """Expose classified measurement failures and refuse a successful report."""
    if record["status"] != "OK":
        print(json.dumps(record), flush=True)
        raise SystemExit(1)


def check_answer(record, phase):
    """Check data answers or the exact kind of successful lifecycle operation."""
    require_success(record)
    prefix = phase.get("expected_status")
    if prefix is not None:
        if len(record["rows"]) != 1:
            raise ValueError(record)
        if not record["rows"][0]["status"].startswith(prefix):
            raise ValueError(record)
        return
    require_rows(record["rows"], phase["expected"])


def preparation(phase, index):
    """Rebuild from no existing view for every timed or warmup CREATE."""
    if phase["name"] == "build" and index != 0:
        return [DROP]
    return []


def worker_payload(config, phase, prefix, index):
    """Describe exactly one cold target and its untimed lifecycle preparation."""
    return {
        "config": str(config),
        "sql": phase["sql"],
        "substitution": phase["substitution"],
        "before": preparation(phase, index),
        "log": str(prefix) + ".server.log",
    }


def cold_sample(output, config, phase, index):
    """Ask the same compare.py runner for one isolated process/runtime sample."""
    prefix = output / f"{phase['name']}-cold-{index}"
    payload = worker_payload(config, phase, prefix, index)
    command = [
        sys.executable,
        str(ROOT / "benchmarks/perf_compare/compare.py"),
        "studio-accelerator",
        "--worker",
    ]
    with Path(str(prefix) + ".worker.log").open("w") as log:
        result = subprocess.run(
            command,
            input=json.dumps(payload),
            text=True,
            stdout=subprocess.PIPE,
            stderr=log,
            check=True,
        )
    return json.loads(result.stdout)


def save_sample(report, phase, mode, index, record):
    """Retain raw samples, including errors, before validating their answers."""
    record.update({"phase": phase["name"], "mode": mode, "index": index})
    report["samples"].append(record)
    check_answer(record, phase)


def warmup(client, sql):
    """Run a genuinely untimed warmup with loud budget/error classification."""
    import psycopg2

    try:
        return {"status": "OK", "rows": execute(client, sql)}
    except psycopg2.Error as error:
        status = "ERROR"
        if "planning budget exceeded" in str(error).lower():
            status = "KILLED"
        return {"status": status, "message": str(error)}


def warm_samples(args, client, report, phase):
    """Use the same live runtime for all warmups, warm samples, and phases."""
    for index in range(args.warmups + args.runs):
        mode, record = warm_sample(args, client, phase, index)
        save_sample(report, phase, mode, index, record)


def warm_sample(args, client, phase, index):
    """Prepare a warm lifecycle target, then select untimed or timed execution."""
    for sql in preparation(phase, args.cold_runs + index):
        execute(client, sql)
    if index < args.warmups:
        return "warmup", warmup(client, phase["sql"])
    return "warm", measure(client, phase["sql"])


def measure_phase(args, output, config, client, report, phase):
    """Always measure cold samples and warm medians for a complete phase."""
    print(f"Measuring {phase['name']}", flush=True)
    for index in range(args.cold_runs):
        record = cold_sample(output, config, phase, index)
        save_sample(report, phase, "cold", index, record)
    warm_samples(args, client, report, phase)


def check_plan(name, plan, fragments):
    """Require actual substitution for cached phases and three sources for live ones."""
    if name in ("cached", "stale", "refreshed"):
        require_cached_plan(plan, fragments)
        return
    require_live_sources(name, fragments)


def require_cached_plan(plan, fragments):
    """Check both the planned replacement and the executed cached-data fragment."""
    lines = []
    for row in plan:
        lines.append(row["plan"])
    text = "\n".join(lines)
    if f"MaterializedScan [{VIEW}]" not in text:
        raise ValueError(f"No materialized substitution: {text}")
    require_cached_fragment(fragments)


def require_live_sources(name, fragments):
    """Require all three real datasource branches for a live computation."""
    sources = set()
    for fragment in fragments:
        sources.add(fragment["datasource"])
    if sources != {"billing", "crm", "gameplay"}:
        raise ValueError((name, fragments))


def require_cached_fragment(fragments):
    """Reject a cache claim unless the sole data-plane fragment is materialized."""
    if len(fragments) != 1:
        raise ValueError(fragments)
    if not fragments[0]["datasource"].startswith("accel:"):
        raise ValueError(fragments)


def capture(client, query, name, expected):
    """Capture the plan, actual answer, emitted fragments, and view metadata."""
    plan = execute(client, "EXPLAIN " + query)
    rows = execute(client, query)
    require_rows(rows, expected)
    fragments = execute(client, "SHOW PUSHDOWN FRAGMENTS")
    views = execute(client, "SHOW MATERIALIZED VIEWS")
    check_plan(name, plan, fragments)
    return {"plan": plan, "rows": rows, "fragments": fragments, "views": views}


def archive_chunks(output, name):
    """Keep exact persisted Arrow chunk bytes and hashes as inspection artifacts."""
    destination = output / "chunks" / name
    destination.mkdir(parents=True)
    records = []
    paths = sorted((output / "state").glob("databases/*/materialized/**/*.arrow"))
    if not paths:
        raise FileNotFoundError("No materialized Arrow chunks found")
    for index, path in enumerate(paths):
        records.append(archive_chunk(path, destination, index, output))
    return records


def archive_chunk(path, destination, index, output):
    """Copy one complete framed Arrow chunk and record its original identity."""
    content = path.read_bytes()
    (destination / f"{index}.arrow").write_bytes(content)
    return {
        "file": str(path.relative_to(output)),
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }
