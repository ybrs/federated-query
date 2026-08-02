"""Check that every plan the engine can reach for a query agrees with the oracle.

The corpus runs each query once, under whatever plan the optimizer chose. That is
one sample of the plan space per query, so a plan reachable only under different
settings has never been executed. This module forces the optimizer down other
paths and holds every resulting plan to the same reference answer.

Comparing plans to EACH OTHER would be weaker: it catches disagreement but not
plans that are unanimously wrong. Every variant is compared to the DuckDB oracle,
which is the same reference the corpus itself uses.

WHY THIS IS AFFORDABLE, and what it costs. Most settings do not change most
plans, so a variant is executed only when it produces a plan this query has not
already run, decided by an EXPLAIN fingerprint - which plans without executing at
0.7ms against 5.9ms to execute. Across the corpus roughly four of every six
variants reproduce a plan already covered.

SETTINGS CHANGE ONCE PER VARIANT, NOT ONCE PER QUERY. A `SET` costs about what
executing a query costs, because the engine persists every override to its store,
so changing settings per query would dominate everything else. Each variant
therefore applies its settings once, walks the whole corpus, and restores them.

WHAT A FINGERPRINT IS. The EXPLAIN tree with the estimate annotations removed -
costs, row counts and the statistics provenance - leaving the operators, their
keys and the SQL each source is sent. Two plans with the same fingerprint do the
same work; the estimates that led there are not part of what is executed.
"""

import re

import os
import time

import pyarrow as pa

from tests.e2e_federated import known_plan_disagreements

# The estimate annotations EXPLAIN carries. They record how the planner got to a
# plan, not what the plan does, so two plans that differ only here are the same
# execution and are not worth running twice.
_ESTIMATE_NOISE = re.compile(r"(cost=[0-9.]+ms|rows=[0-9]+|stats=[^:]*?(?= ::|$))")

# The planner settings each variant applies. Every one reaches plans the default
# does not: turning a rewrite off, forcing the greedy join order, or removing the
# learned statistics that decide join order when a source supplies no NDV.

VARIANTS = (
    ("predicate_pushdown_off", (("optimizer.enable_predicate_pushdown", "false"),)),
    ("projection_pushdown_off", (("optimizer.enable_projection_pushdown", "false"),)),
    ("join_reordering_off", (("optimizer.enable_join_reordering", "false"),)),
    ("join_order_greedy", (("optimizer.max_join_reorder_size", "1"),)),
    ("learned_statistics_on", (("optimizer.enable_learned_statistics", "true"),)),
    ("accelerator_off", (("accelerator.enable_substitution", "false"),)),
)

# Two settings that look like variants are NOT in that list.
#
# `optimizer.enable_decorrelation = false` changes nothing: decorrelation runs
# unconditionally and no production path reads the flag. Measured over the whole
# corpus it changed 0 of 86,352 plans, so as a variant it reported passing while
# exercising nothing - which reads exactly like coverage and is not.
#
# `optimizer.enable_single_source_pushdown = false` is NOT in that list. It is the
# widest variant there could be - with no subtree collapsed into a source query,
# the coordinator computes every operator from plain scans, which is the one plan
# that depends on no pushdown decision at all. It is absent because the engine
# cannot execute that plan: a same-source join lowers to a bare RemoteJoin, which
# panics at step-building because the column-alias map it needs is built by the
# very pushdown that is switched off. The setting exists and works; the plan it
# selects does not run.


def fingerprint(runtime, sql):
    """Return the plan's shape for `sql`, with estimate annotations removed."""
    rows = pa.table(runtime.execute("EXPLAIN " + sql)).to_pylist()
    lines = []
    for row in rows:
        lines.append(_ESTIMATE_NOISE.sub("", row["plan"]).rstrip())
    return "\n".join(lines)


def apply_settings(runtime, settings):
    """Apply one variant's settings to the shared runtime."""
    for name, value in settings:
        runtime.execute("SET " + name + " = " + value)


def reset_settings(runtime, settings):
    """Undo one variant's settings, so the next test sees the suite's defaults."""
    for name, _value in settings:
        runtime.execute("RESET " + name)


class VariantRun:
    """One variant applied once, then held against the oracle over many queries.

    The settings are applied on entry and restored on exit, so a walk of the whole
    corpus pays for two `SET` statements rather than two per query. Disagreements
    are collected rather than raised one at a time, so a run reports every query a
    variant got wrong instead of only the first.
    """

    def __init__(self, runtime, name, settings, compare):
        """Store the engine, the variant being run, and the comparison to hold to."""
        self.runtime = runtime
        self.name = name
        self.settings = settings
        self.compare = compare
        self.considered = 0
        self.executed = 0
        self.already_covered = 0
        self.unplannable = 0
        self.known_disagreements = 0
        self.failures = []
        self.started = time.time()
        # Where the progress lines go. The runner points every process at its own
        # log; unset, they go beside the suite so a single run is still visible.
        self.progress_path = os.environ.get(
            "FEDQ_PROGRESS_LOG", "plan-variant-progress.log"
        )

    def __enter__(self):
        """Apply this variant's settings to the runtime."""
        apply_settings(self.runtime, self.settings)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Restore the suite's settings, whatever happened inside the walk."""
        reset_settings(self.runtime, self.settings)
        return False

    def check(self, sql, default_shape, oracle_table, label, order_sensitive,
              case_name, distribution_name):
        """Compare this variant's plan for one query, if it is a plan not yet run.

        `default_shape` is the fingerprint of the plan the suite's own settings
        produce, which the corpus has already executed and compared.
        """
        self.considered += 1
        self._report_progress()
        shape = self._fingerprint(sql)
        if shape is None:
            return
        if shape == default_shape:
            self.already_covered += 1
            return
        self.executed += 1
        reason = known_plan_disagreements.lookup(
            self.name, case_name, distribution_name
        )
        self._compare_one(sql, oracle_table, label, order_sensitive, reason)

    def _report_progress(self):
        """Append a progress line every few thousand queries.

        A variant is ONE test that walks the whole corpus, so pytest prints
        nothing at all until it finishes - for twenty minutes a running process
        and a hung one look identical. This writes where it has got to, so the
        log answers that.
        """
        if self.considered % 5000 != 0:
            return
        elapsed = time.time() - self.started
        line = (
            "%s progress: %d queries, %d plans executed, %d already covered, "
            "%.0fs elapsed\n"
            % (self.name, self.considered, self.executed, self.already_covered,
               elapsed)
        )
        handle = open(self.progress_path, "a")
        handle.write(line)
        handle.close()

    def _fingerprint(self, sql):
        """This variant's plan shape, or None when it cannot plan the query.

        A setting that removes a rewrite the query needs makes the query
        unplannable rather than differently planned. That is a property of the
        setting, not a defect, and it is counted so the number of variants that
        never ran is a number rather than an assumption.
        """
        try:
            return fingerprint(self.runtime, sql)
        except RuntimeError:
            self.unplannable += 1
            return None

    def _compare_one(self, sql, oracle_table, label, order_sensitive, reason):
        """Execute this variant's plan and hold it to the oracle.

        A pair recorded as a known disagreement is held STRICTLY the other way
        round: it must still disagree, so the entry deletes itself in the change
        that fixes it.
        """
        disagreed = self._disagreement(sql, oracle_table, label, order_sensitive)
        if reason is None:
            if disagreed is not None:
                self.failures.append(disagreed)
            return
        self.known_disagreements += 1
        if disagreed is None:
            self.failures.append(
                label + ": recorded as a known disagreement but now agrees with "
                "the oracle; delete its entry from known_plan_disagreements"
            )

    def _disagreement(self, sql, oracle_table, label, order_sensitive):
        """Return how this variant's plan differed from the oracle, or None."""
        try:
            engine_table = pa.table(self.runtime.execute(sql))
            self.compare(engine_table, oracle_table, label, order_sensitive)
        except (AssertionError, RuntimeError) as error:
            return label + ": " + str(error).split("\n")[0]
        return None

    def summary(self):
        """Return the one-line count of what this variant covered."""
        return (
            "%s: %d queries, %d plans differed and were executed, "
            "%d reproduced the default plan, %d unplannable, "
            "%d known disagreements, %d unexpected"
            % (
                self.name,
                self.considered,
                self.executed,
                self.already_covered,
                self.unplannable,
                self.known_disagreements,
                len(self.failures),
            )
        )
