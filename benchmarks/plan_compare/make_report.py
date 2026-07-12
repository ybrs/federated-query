"""Compare old vs new pushed-SQL dumps and write REPORT.md.

Reads benchmarks/plan_compare/{old,new}/<suite>/<q>.txt (produced by dump_old.py
and dump_new.py) and, per suite, emits a per-query divergence table and a ranked
"systematic divergences" section grouping queries by shared root cause.

Structural signals compared (never raw SQL strings):
  - island count and the multiset of (source, pushdown|rawscan) per query
  - whether any island pushes an aggregate/join
  - dim shipments (islands that read a __fedq_ship temp table)
  - whether the new engine errored (the error text is the finding)
"""

import glob
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
SHIP_RE = re.compile(r"__fedq_ship_\d+")


def _parse_file(path):
    """Parse one dump file into a structured record."""
    text = open(path).read()
    record = {"error": None, "islands": [], "ship": 0}
    error = re.search(r"^ERROR: (.+)$", text, re.MULTILINE)
    if error:
        record["error"] = error.group(1).strip()
    for match in re.finditer(
        r"^--- island \d+ source=(\S+) kind=(\S+) ---$", text, re.MULTILINE
    ):
        record["islands"].append((match.group(1), match.group(2)))
    record["ship"] = len(set(SHIP_RE.findall(text)))
    record["pushes"] = any(kind in ("join", "agg") for _s, kind in record["islands"])
    return record


def _island_multiset(islands):
    """A hashable sorted multiset of (source, kind) island signatures."""
    return tuple(sorted(islands))


def _divergence(old, new):
    """Return (is_divergent, one-phrase reason) comparing old vs new records."""
    if new["error"]:
        return True, "NEW errors: " + _short_error(new["error"])
    if (old["ship"] > 0) != (new["ship"] > 0):
        if old["ship"] > 0:
            return True, "OLD ships dim into duck, NEW does not"
        return True, "NEW ships dim into duck, OLD does not"
    if old["pushes"] != new["pushes"]:
        if old["pushes"]:
            return True, "OLD pushes agg/join, NEW leaves it in coordinator"
        return True, "NEW pushes agg/join, OLD leaves it in coordinator"
    if _island_multiset(old["islands"]) != _island_multiset(new["islands"]):
        return True, "different island split ({0} vs {1} islands)".format(
            len(old["islands"]), len(new["islands"]))
    return False, ""


def _short_error(text):
    """Condense an error string to its distinguishing tail."""
    text = text.replace("RuntimeError: ", "").replace("PanicException: ", "panic: ")
    return text[:70]


# Root-cause buckets for the systematic-divergence section. Each entry is
# (label, predicate over (reason, old, new)). First match wins; order matters.
def _pattern_label(reason, old, new):
    """Classify a divergence into a systematic root-cause bucket."""
    if "window_function" in reason:
        return "NEW parse gap: window functions unsupported"
    if "stddev_samp" in reason:
        return "NEW parse gap: stddev_samp unsupported"
    if "Implicit JOIN" in reason:
        return "NEW parse gap: comma/implicit JOIN unsupported"
    if "function `union`" in reason:
        return "NEW parse gap: UNION in this position unsupported"
    if "rollup" in reason:
        return "NEW planning gap: ROLLUP unsupported"
    if "type_coercion" in reason:
        return "NEW execution gap: type_coercion failure"
    if "parameterless aggregate" in reason:
        return "NEW pushdown bug: COUNT() rendered without * to Postgres"
    if "ColumnRef type must be set" in reason:
        return "NEW panic: ColumnRef type unset during binding"
    if "ships dim" in reason:
        return reason
    if "pushes agg/join" in reason:
        return reason
    if "different island split" in reason:
        return "different island split (join/reduction placement differs)"
    return reason


def _load_suite(suite):
    """Load matched old/new records for every query in a suite."""
    old_dir = os.path.join(HERE, "old", suite)
    new_dir = os.path.join(HERE, "new", suite)
    names = []
    for path in sorted(glob.glob(os.path.join(old_dir, "q*.txt"))):
        names.append(os.path.splitext(os.path.basename(path))[0])
    records = []
    for name in names:
        old = _parse_file(os.path.join(old_dir, name + ".txt"))
        new_path = os.path.join(new_dir, name + ".txt")
        new = _parse_file(new_path) if os.path.exists(new_path) else {
            "error": "no new dump", "islands": [], "ship": 0, "pushes": False}
        diverges, reason = _divergence(old, new)
        records.append((name, old, new, diverges, reason))
    return records


def _bool(value):
    """Render a boolean as yes/no."""
    return "yes" if value else "no"


def _suite_table(records):
    """Build the per-query markdown table lines for a suite."""
    lines = [
        "| query | old #isl | new #isl | old ship | new ship | old push | "
        "new push | new err | divergence |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for name, old, new, diverges, reason in records:
        err = _bool(new["error"] is not None)
        div = "no" if not diverges else "YES: " + reason
        lines.append(
            "| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} |".format(
                name, len(old["islands"]), len(new["islands"]),
                old["ship"], new["ship"], _bool(old["pushes"]),
                _bool(new["pushes"]), err, div))
    return lines


def _example_for(records, label):
    """Find a concrete (name, old, new) example whose divergence maps to label."""
    for name, old, new, diverges, reason in records:
        if diverges and _pattern_label(reason, old, new) == label:
            return name, old, new
    return None


def _example_sql(suite, engine, name):
    """Return a representative island SQL line for a query from a dump file."""
    path = os.path.join(HERE, engine, suite, name + ".txt")
    text = open(path).read()
    error = re.search(r"^ERROR: (.+)$", text, re.MULTILINE)
    if error:
        return "ERROR: " + error.group(1).strip()
    blocks = re.split(r"^--- island .*$", text, flags=re.MULTILINE)[1:]
    summary = []
    for block in blocks:
        first = block.strip().splitlines()[0] if block.strip() else ""
        summary.append(first[:200])
    return " || ".join(summary) if summary else "(no islands)"


def _systematic_section(suite, records):
    """Rank divergence patterns by query count with one concrete example each."""
    buckets = {}
    for name, old, new, diverges, reason in records:
        if not diverges:
            continue
        label = _pattern_label(reason, old, new)
        buckets.setdefault(label, []).append(name)
    ordered = sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))
    lines = ["", "### Systematic divergences ({0})".format(suite), ""]
    for label, names in ordered:
        lines.append("**{0}** - {1} queries: {2}".format(
            label, len(names), ", ".join(names)))
        example = names[0]
        lines.append("- example {0}:".format(example))
        lines.append("  - OLD: {0}".format(_example_sql(suite, "old", example)))
        lines.append("  - NEW: {0}".format(_example_sql(suite, "new", example)))
        lines.append("")
    return lines, ordered


def _counts(records):
    """(#compared, #divergent, #new-errored) for a suite's records."""
    compared = len(records)
    divergent = sum(1 for _n, _o, _nw, d, _r in records if d)
    errored = sum(1 for _n, _o, new, _d, _r in records if new["error"])
    return compared, divergent, errored


def main():
    """Build REPORT.md across both suites and print the headline counts."""
    lines = ["# Plan comparison: OLD (python) vs NEW (rust) engine", "",
             "Federated split pg-dims (dims on PostgreSQL, facts on DuckDB). "
             "TPC-H scale 0.01, TPC-DS scale 0.1. Structural comparison of the "
             "SQL each engine pushes to each source.", ""]
    summary = {}
    for suite in ("tpch", "tpcds"):
        records = _load_suite(suite)
        compared, divergent, errored = _counts(records)
        summary[suite] = (compared, divergent, errored)
        lines.append("## {0}: {1} compared, {2} divergent, {3} new-errored".format(
            suite.upper(), compared, divergent, errored))
        lines.append("")
        lines.extend(_suite_table(records))
        section, _ordered = _systematic_section(suite, records)
        lines.extend(section)
    with open(os.path.join(HERE, "REPORT.md"), "w") as handle:
        handle.write("\n".join(lines))
    for suite in ("tpch", "tpcds"):
        compared, divergent, errored = summary[suite]
        print("{0}: {1} compared, {2} divergent, {3} errored".format(
            suite, compared, divergent, errored))


if __name__ == "__main__":
    main()
