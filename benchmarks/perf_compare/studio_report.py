"""Persist raw accelerator measurements and an honest cold/warm report."""

from datetime import datetime, timezone
import hashlib
import json
import platform
import statistics
import subprocess

from wire_runtime import ROOT, encode_decimal


def metadata(args):
    """Describe the exact request, source fixture, and release-only measurement path."""
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_dirty": bool(git_output("status", "--porcelain")),
        "sha256": provenance(),
        "platform": platform.platform(),
        "args": vars(args),
        "query_transport": "PostgreSQL wire",
        "cold": "fresh Python worker + release server + runtime per sample",
        "warm": "one live server connection across all phases",
        "learned_statistics": False,
        "planning_budget": "engine default",
        "sql_trace_enabled": True,
        "fragment_recording": True,
    }


def git_output(*arguments):
    """Read version-control evidence without hiding a failed Git invocation."""
    return subprocess.run(
        ["git", *arguments], cwd=ROOT, text=True, stdout=subprocess.PIPE, check=True
    ).stdout.strip()


def provenance():
    """Hash the release server, harness, SQL, and original source fixtures."""
    hashes = {}
    for path in provenance_files():
        hashes[str(path.relative_to(ROOT))] = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
    return hashes


def provenance_files():
    """Enumerate the implementation and data inputs that determine this experiment."""
    directory = ROOT / "benchmarks" / "perf_compare"
    paths = sorted(directory.glob("studio_*.py"))
    paths.extend(
        [
            directory / "compare.py",
            directory / "wire_runtime.py",
            ROOT / "target/release/fedq-server",
        ]
    )
    paths.extend(fixture_files())
    return paths


def fixture_files():
    """Identify the original data files and exact SQL used to change their state."""
    studio = ROOT / "tutorials/game-studio"
    paths = []
    for name in ("players", "tickets", "events"):
        paths.append(studio / "generated" / f"{name}.parquet")
    paths.extend(
        [
            studio / "queries/06_accelerator_support.sql",
            studio / "accelerator-insert.sql",
            studio / "accelerator-undo.sql",
            studio / "compose.yaml",
        ]
    )
    return paths


def phase_summary(samples, phase):
    """Compute cold and warm medians from every successful raw timed sample."""
    values = {"cold": [], "warm": []}
    for sample in samples:
        if sample["phase"] != phase:
            continue
        if sample["mode"] in values:
            values[sample["mode"]].append(sample["ms"])
    return {
        "cold_ms": statistics.median(values["cold"]),
        "warm_ms": statistics.median(values["warm"]),
        "cold_samples_ms": values["cold"],
        "warm_samples_ms": values["warm"],
    }


def summaries(report):
    """Summarize every phase only after the full experiment and cleanup pass."""
    result = {}
    for name in ("live", "build", "cached", "stale", "bypass", "refresh", "refreshed"):
        result[name] = phase_summary(report["samples"], name)
    return result


def report_lines(report):
    """Explain measurement boundaries and show both mandatory temperature columns."""
    lines = report_header(report)
    for name, result in report.get("summary", {}).items():
        lines.append(f"| {name} | {result['cold_ms']:.3f} | {result['warm_ms']:.3f} |")
    lines.extend(
        [
            "",
            "Raw samples, answers, source fragments, and refresh statuses "
            "are retained in results.json. Server/worker logs and Arrow chunks "
            "are retained alongside it.",
        ]
    )
    return lines


def report_header(report):
    """State cold/warm semantics before presenting any latency numbers."""
    return [
        "# Game-studio accelerator: actual wire-protocol measurements",
        "",
        f"Completed: {report['completed']}",
        "",
        "Release server; statement issue through full result decoding. "
        "Startup, fixture preparation, and DROP before CREATE are outside timing.",
        "Cold means fresh processes/runtime, not an emptied OS or database cache.",
        "Warm values are medians on one live server connection. "
        "Learned-statistics reads are disabled; the planning budget is unchanged.",
        "CREATE and REFRESH include all work inside those statements. "
        "SQL tracing and fragment recording are enabled throughout.",
        "",
        "| Phase | Cold median ms | Warm median ms |",
        "| --- | ---: | ---: |",
    ]


def save_report(output, report):
    """Keep failures and successful results, retaining unrounded JSON measurements."""
    rendered = json.dumps(report, indent=2, default=encode_decimal) + "\n"
    (output / "results.json").write_text(rendered, encoding="utf8")
    (output / "REPORT.md").write_text(
        "\n".join(report_lines(report)) + "\n", encoding="ascii"
    )
