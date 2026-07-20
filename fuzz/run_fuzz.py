"""Driver for the federated query fuzzer.

Runs a time-bounded fuzzing session that interleaves the typed generator and the
corpus-seeded mutator, feeding every query through the oracle stack. It verifies
the settings variants against live plans at startup, prints a live stat line every
30 seconds, dedupes findings by signature, and writes each finding to
``fuzz/artifacts/<timestamp>/`` as a ready-to-park corpus dict plus a greedily
shrunk repro and an exact replay command. The run is deterministic in ``--seed``:
the same seed reproduces the same generation/mutation stream, so ``--replay
<index>`` re-runs the single work item that produced a finding.

Usage::

    python -m fuzz.run_fuzz --minutes 5 --seed 1 --profile smoke
    python -m fuzz.run_fuzz --seed 1 --profile smoke --replay 42
"""

import argparse
import datetime
import os
import random
import sys
import time

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from hypothesis import HealthCheck, Phase, given, seed, settings  # noqa: E402

from fuzz import generator, mutator, oracles, reducer  # noqa: E402

_PROFILES = {
    "smoke": {"gen_batch": 20, "watchdog_seconds": 15, "minutes": 5},
    "deep": {"gen_batch": 30, "watchdog_seconds": 30, "minutes": 25},
}


def _parse_args(argv):
    """Parse the command-line arguments for a fuzzing session or a replay."""
    parser = argparse.ArgumentParser(description="Federated query fuzzer")
    parser.add_argument("--minutes", type=float, default=None)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--profile", choices=sorted(_PROFILES), default="smoke")
    parser.add_argument("--replay", type=int, default=None)
    return parser.parse_args(argv)


def _seed_value(base_seed, batch_no):
    """Return the deterministic hypothesis seed for one generation batch."""
    return base_seed * 100003 + batch_no


def _draw_batch(seed_value, count):
    """Draw ``count`` generated queries deterministically for a seed value."""
    sink = []
    decorated = _batch_collector(sink, seed_value, count)
    decorated()
    return sink


def _batch_collector(sink, seed_value, count):
    """Build the seeded, generate-only hypothesis collector for a batch."""

    @seed(seed_value)
    @settings(
        max_examples=count,
        phases=[Phase.generate],
        database=None,
        deadline=None,
        suppress_health_check=list(HealthCheck),
    )
    @given(generator.queries())
    def collect(query):
        """Record one drawn query into the batch sink."""
        sink.append(query)

    return collect


def iter_work(base_seed, profile):
    """Yield an endless deterministic stream of (kind, payload) work items."""
    rng = random.Random(base_seed)
    corpus = mutator.seed_cases()
    batch_no = 0
    while True:
        for query in _draw_batch(
            _seed_value(base_seed, batch_no), profile["gen_batch"]
        ):
            yield ("gen", query)
        batch_no += 1
        for mutation in mutator.mutations(rng, rng.choice(corpus)):
            yield ("mut", mutation)


def _run_work(ctx, kind, payload):
    """Dispatch one work item through the oracle stack."""
    if kind == "gen":
        ctx.stats.generated += 1
        oracles.check_query(ctx, payload)
        return
    ctx.stats.mutated += 1
    oracles.check_query(ctx, payload.mutated)
    if payload.kind == "preserving":
        oracles.check_metamorphic(ctx, payload.original, payload.mutated)


def _safe_run_work(ctx, kind, payload):
    """Run one work item, surfacing a harness error loudly without ending the run."""
    try:
        _run_work(ctx, kind, payload)
    except Exception as error:  # noqa: a harness bug is printed loudly, not hidden
        sys.stdout.write("HARNESS-ERROR " + kind + ": " + repr(error) + "\n")
        sys.stdout.flush()


def _connect_pg():
    """Open the shared PostgreSQL connection, or return None when pg is skipped."""
    if os.environ.get("FEDQ_E2E_SKIP_PG") == "1":
        return None
    import psycopg2

    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "test_db"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    connection.autocommit = True
    return connection


def _build_context(base_seed, profile, connection):
    """Verify the variants and build the shared oracle context."""
    skip_pg = connection is None
    live, reports = oracles.verify_variants(connection, skip_pg)
    _print_variant_reports(reports)
    rng = random.Random(base_seed + 7)
    ctx = oracles.OracleContext(
        connection, skip_pg, rng, profile["watchdog_seconds"], live
    )
    return ctx, reports


def _print_variant_reports(reports):
    """Print each settings variant's live/dead/control classification."""
    sys.stdout.write("settings variants (plan-change verified at startup):\n")
    for report in reports:
        line = (
            "  "
            + report.name
            + " ["
            + report.role
            + "] "
            + report.status
            + " - "
            + report.evidence
        )
        sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _session(ctx, base_seed, profile, minutes):
    """Run the timed fuzzing loop, printing a live stat line every 30 seconds."""
    deadline = time.time() + minutes * 60.0
    last_print = time.time()
    index = 0
    for kind, payload in iter_work(base_seed, profile):
        if time.time() >= deadline:
            break
        before = ctx.stats.findings
        _safe_run_work(ctx, kind, payload)
        _tag_new_findings(ctx, before, index)
        last_print = _maybe_print(ctx, last_print)
        index += 1
    return index


def _tag_new_findings(ctx, before, index):
    """Attach the discovery work index to any finding first seen this step."""
    if ctx.stats.findings == before:
        return
    for finding in ctx.findings.values():
        if not hasattr(finding, "discovery_index"):
            finding.discovery_index = index


def _maybe_print(ctx, last_print):
    """Print the live stat line when 30 seconds have elapsed; return the new mark."""
    now = time.time()
    if now - last_print < 30.0:
        return last_print
    sys.stdout.write(ctx.stats.line() + "\n")
    sys.stdout.flush()
    return now


def _artifact_dir():
    """Create and return a fresh timestamped artifacts directory."""
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "artifacts", stamp)
    os.makedirs(path, exist_ok=True)
    return path


def _reduce_finding(ctx, finding):
    """Greedily shrink a reducible finding's query; others keep their query."""
    if finding.category not in ("mismatch", "surface_gap"):
        return finding.query
    predicate = _reduction_predicate(ctx, finding)
    return reducer.reduce_query(finding.query, predicate)


def _reduction_predicate(ctx, finding):
    """Build the reducer predicate that re-checks a candidate for the same category."""

    def predicate(candidate_sql):
        """Whether the candidate reproduces the finding's category."""
        return oracles.recheck_reproduces(
            ctx, candidate_sql, finding.tables, False, finding.category
        )

    return predicate


def _write_findings(ctx, base_seed, profile_name):
    """Write every deduped finding to the artifacts directory, reduced where able."""
    if not ctx.findings:
        return None
    directory = _artifact_dir()
    number = 0
    for finding in ctx.findings.values():
        number += 1
        _write_one_finding(ctx, directory, number, finding, base_seed, profile_name)
    return directory


def _write_one_finding(ctx, directory, number, finding, base_seed, profile_name):
    """Write one finding artifact: corpus dict, shrunk repro, and replay command."""
    reduced = _reduce_finding(ctx, finding)
    text = _finding_text(finding, reduced, base_seed, profile_name)
    path = os.path.join(
        directory, "finding_" + str(number).zfill(2) + "_" + finding.category + ".py"
    )
    handle = open(path, "w")
    handle.write(text)
    handle.close()


def _finding_text(finding, reduced, base_seed, profile_name):
    """Render the artifact file body for one finding."""
    name = (
        "fuzz_" + finding.category + "_" + str(getattr(finding, "discovery_index", 0))
    )
    case = finding.as_corpus_case(name)
    replay = _replay_command(finding, base_seed, profile_name)
    lines = [
        "# " + finding.category + " finding",
        "# detail: " + finding.detail.replace("\n", " "),
    ]
    lines.append("# replay: " + replay)
    lines.append("# shrunk repro query:")
    lines.append("# " + reduced)
    lines.append("CASE = " + repr(case))
    return "\n".join(lines) + "\n"


def _replay_command(finding, base_seed, profile_name):
    """Build the exact command that re-runs the finding's work item."""
    index = getattr(finding, "discovery_index", 0)
    return (
        "python -m fuzz.run_fuzz --seed "
        + str(base_seed)
        + " --profile "
        + profile_name
        + " --replay "
        + str(index)
    )


def _report(ctx, artifact_dir):
    """Print the final session report: stats, artifacts, and every finding."""
    sys.stdout.write("\n=== session complete ===\n")
    sys.stdout.write(ctx.stats.line() + "\n")
    if artifact_dir is not None:
        sys.stdout.write("artifacts: " + artifact_dir + "\n")
    for finding in ctx.findings.values():
        head = finding.detail.splitlines()[0]
        sys.stdout.write(
            "FINDING "
            + finding.category
            + " ["
            + finding.placement
            + "/"
            + finding.variant
            + "] "
            + head
            + "\n"
        )
    sys.stdout.flush()


def _replay(ctx, base_seed, profile, target_index):
    """Re-run the single deterministic work item at ``target_index`` with deep checks."""
    ctx.force_deep = True
    index = 0
    for kind, payload in iter_work(base_seed, profile):
        if index == target_index:
            _safe_run_work(ctx, kind, payload)
            break
        index += 1
    sys.stdout.write(
        "replay index "
        + str(target_index)
        + " -> findings="
        + str(ctx.stats.findings)
        + "\n"
    )
    _report(ctx, None)


def main(argv):
    """Entry point: run a fuzzing session or a single-item replay."""
    args = _parse_args(argv)
    profile = _PROFILES[args.profile]
    minutes = args.minutes if args.minutes is not None else profile["minutes"]
    connection = _connect_pg()
    ctx, _reports = _build_context(args.seed, profile, connection)
    if args.replay is not None:
        _replay(ctx, args.seed, profile, args.replay)
        return
    _session(ctx, args.seed, profile, minutes)
    artifact_dir = _write_findings(ctx, args.seed, args.profile)
    _report(ctx, artifact_dir)


if __name__ == "__main__":
    main(sys.argv[1:])
