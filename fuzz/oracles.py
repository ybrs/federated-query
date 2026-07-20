"""The check stack the fuzzer runs on one generated or mutated query.

For each query the stack applies, in order: a BASELINE run on the single-DuckDB
oracle (a query the oracle rejects is discarded, not a bug); PLACEMENT INVARIANCE
(run through the engine under a sampled set of data-source placements and compare
each to the oracle ground truth); SETTINGS INVARIANCE (re-run under optimizer-
config variants that must not change the answer); TLP (split a ternary predicate
and check the partitions reunite); and the CONTRACT rule that the only acceptable
engine outcomes are a value-match or a known designed raise. Anything else - a
mismatch, a panic, a hang, or an undocumented raise on oracle-accepted SQL - is a
Finding.

Environments and oracles are cached exactly as the e2e suite caches them, keyed by
(table set, placement); the PostgreSQL environments reuse the shared connection.
"""

import threading

from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import case_table_specs
from tests.e2e_federated.compare import compare_tables
from tests.e2e_federated.conftest import EnvCache
from tests.e2e_federated.oracle import build_oracle
from tests.e2e_federated.placements import PLACEMENTS, placement_uses_postgres

# Engine error substrings that name a designed restriction: a query DuckDB accepts
# but the engine raises on for one of these is a known surface limit, discarded
# (and counted) rather than reported as a bug.
KNOWN_ENGINE_ERRORS = (
    "cross-source NATURAL/USING",
    "unsupported SQL: query",
    "unsupported position",
    "ordered-set aggregate",
    "not found in table",
    "not found in scope",
    "not allowed in WHERE",
    "must appear in the GROUP BY",
    "function `tuple`",
    "exactly one column",
    "ORDER BY inside STRING_AGG",
    "ambiguous",
    "LATERAL",
    "correlated",
)

# Planning-budget kill markers: these are by-design under the O(metadata) budget,
# so a kill is retried once and, if deterministic, discarded rather than reported.
_BUDGET_MARKERS = ("planning budget", "PlanBudget", "budget")

# The optimizer-config variants the settings oracle sweeps. Each MUST leave the
# answer unchanged; a mismatch names the guilty optimization. ``role`` is ``opt``
# for a real optimization toggle or ``control`` for the planning-budget raise that
# is expected never to change a plan or a result.
VARIANTS = (
    (
        "predicate_pushdown_off",
        (("optimizer.enable_predicate_pushdown", "false"),),
        "opt",
    ),
    (
        "projection_pushdown_off",
        (("optimizer.enable_projection_pushdown", "false"),),
        "opt",
    ),
    ("join_reorder_off", (("optimizer.enable_join_reordering", "false"),), "opt"),
    ("join_reorder_goo", (("optimizer.max_join_reorder_size", "1"),), "opt"),
    (
        "dim_shipping_aggressive",
        (
            ("optimizer.ship_local_floor", "0"),
            ("optimizer.ship_row_budget", "100000000"),
            ("optimizer.ship_min_ratio", "1"),
        ),
        "opt",
    ),
    ("decorrelation_off", (("optimizer.enable_decorrelation", "false"),), "opt"),
    ("accelerator_off", (("accelerator.enable_substitution", "false"),), "opt"),
    ("planning_budget_high", (("optimizer.planning_budget_ms", "60000"),), "control"),
)

# Probe queries used once at startup to prove each variant changes at least one
# EXPLAIN plan (a variant that never changes any plan is a dead knob, logged).
PROBE_QUERIES = (
    (
        "SELECT o.order_id AS c0 FROM {orders} o WHERE o.price > 50 AND o.status = 'processing'",
        ["orders"],
    ),
    (
        "SELECT o.order_id AS c0, p.name AS c1, c.city AS c2 FROM {orders} o "
        "JOIN {products} p ON o.product_id = p.product_id "
        "JOIN {customers} c ON o.customer_id = c.customer_id "
        "WHERE o.price > 20 AND c.segment = 'enterprise'",
        ["orders", "products", "customers"],
    ),
    (
        "SELECT o.order_id AS c0, l.description AS c1 FROM {orders} o "
        "JOIN {products} p ON o.product_id = p.product_id "
        "JOIN {customers} c ON o.customer_id = c.customer_id "
        "JOIN {t_lookup} l ON o.status = l.code WHERE c.segment = 'enterprise'",
        ["orders", "products", "customers", "t_lookup"],
    ),
    (
        "SELECT di.dept AS c0, SUM(f.amount) AS c1 FROM {fact_sales} f "
        "JOIN {dim_item} di ON f.item_key = di.item_key GROUP BY di.dept",
        ["fact_sales", "dim_item"],
    ),
)

# Placements probed at startup to classify variants; a knob that reorders joins
# shows on one operand layout but not another, so more than one is probed.
_PROBE_PLACEMENTS = ("duck_pg", "pg_duck")


class Finding:
    """One triage-worthy engine outcome, with everything a repro needs.

    ``category`` is ``mismatch``/``panic``/``hang``/``surface_gap``/``settings``/
    ``tlp``; ``signature`` dedupes findings; the corpus-format fields (name,
    tables, query, finding) let a triaged finding be parked directly.
    """

    def __init__(self, category, placement, variant, detail, query, tables):
        """Store the finding's category, location, detail, and source query."""
        self.category = category
        self.placement = placement
        self.variant = variant
        self.detail = detail
        self.query = query
        self.tables = tables
        self.signature = _signature(category, placement, variant, detail)

    def as_corpus_case(self, name):
        """Render this finding as a ready-to-park e2e corpus case dict."""
        return {
            "name": name,
            "tables": sorted(self.tables),
            "query": self.query,
            "finding": self.category
            + " ["
            + self.placement
            + "/"
            + self.variant
            + "]: "
            + self.detail,
        }


def _signature(category, placement, variant, detail):
    """Build a dedupe signature from the finding's stable parts."""
    return category + "|" + placement + "|" + variant + "|" + _error_class(detail)


def _error_class(detail):
    """Reduce a detail message to a stable class (its leading words)."""
    head = detail.strip().splitlines()[0]
    return head[:60]


class FuzzStats:
    """Running counters printed on the live stat line and in the final report."""

    def __init__(self):
        """Initialize every counter to zero."""
        self.generated = 0
        self.mutated = 0
        self.oracle_accepted = 0
        self.discarded_oracle = 0
        self.discarded_known = 0
        self.discarded_budget = 0
        self.placement_checks = 0
        self.settings_checks = 0
        self.tlp_checks = 0
        self.metamorphic_checks = 0
        self.findings = 0

    def line(self):
        """Return the compact one-line stats summary."""
        produced = "gen=" + str(self.generated) + " mut=" + str(self.mutated)
        accepted = " ok=" + str(self.oracle_accepted)
        discarded = " disc(oracle=" + str(self.discarded_oracle) + ",known="
        discarded += (
            str(self.discarded_known) + ",budget=" + str(self.discarded_budget) + ")"
        )
        checks = " checks(place=" + str(self.placement_checks) + ",set="
        checks += str(self.settings_checks) + ",tlp=" + str(self.tlp_checks) + ")"
        return produced + accepted + discarded + checks + " FIND=" + str(self.findings)


class OracleContext:
    """Everything the check stack shares across queries: caches, pg, config."""

    def __init__(self, pg_connection, skip_pg, rng, watchdog_seconds, live_variants):
        """Store the pg handle, RNG, watchdog budget, and active variant list."""
        self.pg_connection = pg_connection
        self.skip_pg = skip_pg
        self.rng = rng
        self.watchdog_seconds = watchdog_seconds
        self.live_variants = live_variants
        self.env_cache = EnvCache(_env_cap())
        self.oracle_cache = {}
        self.stats = FuzzStats()
        self.findings = {}
        self.force_deep = False


def _env_cap():
    """Return the environment-cache cap the fuzzer keeps live at once."""
    return 16


def _run_with_timeout(func, seconds):
    """Run ``func`` on a daemon thread and raise TimeoutError past ``seconds``."""
    box = {}
    worker = threading.Thread(target=_capture, args=(func, box), daemon=True)
    worker.start()
    worker.join(seconds)
    if worker.is_alive():
        raise TimeoutError("watchdog fired after " + str(seconds) + "s")
    if "error" in box:
        raise box["error"]
    return box["value"]


def _capture(func, box):
    """Run ``func``, storing its value or exception into ``box``."""
    try:
        box["value"] = func()
    except BaseException as error:  # noqa: catch-all is intentional: a Rust panic
        box["error"] = error  # arrives as a BaseException we must record loudly


def get_oracle(ctx, specs, tables):
    """Return the cached single-DuckDB oracle for a query's table set."""
    key = frozenset(tables)
    if key not in ctx.oracle_cache:
        ctx.oracle_cache[key] = build_oracle(specs)
    return ctx.oracle_cache[key]


def get_env(ctx, placement, specs):
    """Return the cached engine environment for (table set, placement)."""
    key = (frozenset(specs.keys()), placement.name)
    if key not in ctx.env_cache:
        ctx.env_cache[key] = runtime_mod.build_environment(
            placement, specs, ctx.pg_connection
        )
    return ctx.env_cache[key]


def _is_known_error(message):
    """Whether an engine error message names a designed surface restriction."""
    for marker in KNOWN_ENGINE_ERRORS:
        if marker in message:
            return True
    return False


def _is_budget_kill(message):
    """Whether an engine error message is a planning-budget kill."""
    for marker in _BUDGET_MARKERS:
        if marker in message:
            return True
    return False


def _engine_execute(ctx, env, sql):
    """Execute a rendered query through the engine under the watchdog."""
    return _run_with_timeout(lambda: env.execute(sql), ctx.watchdog_seconds)


def check_query(ctx, gen_query):
    """Run the full oracle stack on one generated/mutated query."""
    specs = case_table_specs(
        {"name": "fuzz", "tables": gen_query.tables, "query": gen_query.sql}
    )
    oracle_table = _baseline(ctx, specs, gen_query)
    if oracle_table is None:
        return
    ctx.stats.oracle_accepted += 1
    _placement_stage(ctx, gen_query, specs, oracle_table)


def _baseline(ctx, specs, gen_query):
    """Run the query on the oracle; return its table or None if rejected."""
    oracle = get_oracle(ctx, specs, gen_query.tables)
    try:
        return oracle.run(gen_query.sql)
    except Exception:  # noqa: a DuckDB rejection is a legitimate discard, not a bug
        ctx.stats.discarded_oracle += 1
        return None


def _placement_stage(ctx, gen_query, specs, oracle_table):
    """Run the engine under each sampled placement and drive the deeper oracles."""
    good_env = None
    for placement in _sample_placements(ctx, gen_query):
        env = get_env(ctx, placement, specs)
        engine_table = _run_one_placement(ctx, env, placement, gen_query, oracle_table)
        if engine_table is not None and good_env is None:
            good_env = env
    if good_env is not None:
        _deep_oracles(ctx, good_env, gen_query, oracle_table)


def _run_one_placement(ctx, env, placement, gen_query, oracle_table):
    """Execute one placement and compare, classifying any engine failure."""
    ctx.stats.placement_checks += 1
    outcome = _execute_classified(ctx, env, gen_query.sql, placement.name)
    if outcome.table is None:
        _record_execution_outcome(ctx, outcome, placement.name, "-", gen_query)
        return None
    _compare_or_record(ctx, outcome.table, oracle_table, placement.name, "-", gen_query)
    return outcome.table


class _Outcome:
    """The result of one engine execution: a table, or a classified failure."""

    def __init__(self, table, kind, detail):
        """Store the result table (or None) and the failure kind and detail."""
        self.table = table
        self.kind = kind
        self.detail = detail


def _execute_classified(ctx, env, sql, placement_name):
    """Execute once (retrying a budget kill) and classify the outcome."""
    outcome = _execute_attempt(ctx, env, sql)
    if outcome.kind == "budget":
        outcome = _execute_attempt(ctx, env, sql)
    return outcome


def _execute_attempt(ctx, env, sql):
    """One engine execution attempt, mapped to an _Outcome."""
    try:
        return _Outcome(_engine_execute(ctx, env, sql), "ok", "")
    except TimeoutError as error:
        return _Outcome(None, "hang", str(error))
    except BaseException as error:  # noqa: classify every failure, incl. panics
        return _classify_error(error)


def _classify_error(error):
    """Map a raised engine error to an _Outcome kind."""
    message = str(error)
    if "panic" in message.lower() or not isinstance(error, Exception):
        return _Outcome(None, "panic", message)
    if _is_budget_kill(message):
        return _Outcome(None, "budget", message)
    if _is_known_error(message):
        return _Outcome(None, "known", message)
    return _Outcome(None, "surface_gap", message)


def _record_execution_outcome(ctx, outcome, placement_name, variant, gen_query):
    """Turn a non-table outcome into a discard count or a Finding."""
    if outcome.kind == "known":
        ctx.stats.discarded_known += 1
        return
    if outcome.kind == "budget":
        ctx.stats.discarded_budget += 1
        return
    category = "surface_gap" if outcome.kind == "surface_gap" else outcome.kind
    _add_finding(
        ctx,
        Finding(
            category,
            placement_name,
            variant,
            outcome.detail,
            gen_query.sql,
            gen_query.tables,
        ),
    )


def _compare_or_record(
    ctx, engine_table, oracle_table, placement_name, variant, gen_query
):
    """Compare an engine table to the oracle, recording a mismatch Finding."""
    try:
        compare_tables(
            engine_table, oracle_table, placement_name, gen_query.order_sensitive
        )
    except AssertionError as error:
        _add_finding(
            ctx,
            Finding(
                "mismatch",
                placement_name,
                variant,
                str(error),
                gen_query.sql,
                gen_query.tables,
            ),
        )


def check_metamorphic(ctx, original, mutated):
    """Compare the engine's result for a preserving mutation to the original's.

    Both queries run through the engine under one sampled cross-source placement;
    a preserving mutation must not change the multiset of rows. A divergence is a
    metamorphic Finding (a plan-dependent engine bug the mutation exposed).
    """
    specs = case_table_specs(
        {"name": "meta", "tables": original.tables, "query": original.sql}
    )
    placement = _metamorphic_placement(ctx, original)
    if placement is None:
        return
    env = get_env(ctx, placement, specs)
    _run_metamorphic(ctx, env, placement, original, mutated)


def _metamorphic_placement(ctx, original):
    """Pick one applicable placement to run the metamorphic pair on."""
    applicable = _applicable_placements(ctx, original)
    if not applicable:
        return None
    return ctx.rng.choice(applicable)


def _run_metamorphic(ctx, env, placement, original, mutated):
    """Execute the original and mutated queries and compare their engine rows."""
    base = _execute_attempt(ctx, env, original.sql).table
    other = _execute_attempt(ctx, env, mutated.sql).table
    if base is None or other is None:
        return
    ctx.stats.metamorphic_checks += 1
    _compare_metamorphic(ctx, base, other, placement.name, original, mutated)


def _compare_metamorphic(ctx, base, other, placement_name, original, mutated):
    """Assert the mutated engine rows equal the original's as a multiset."""
    if sorted(_table_rows(base)) == sorted(_table_rows(other)):
        return
    detail = (
        "preserving mutation changed rows: base="
        + str(base.num_rows)
        + " mutated="
        + str(other.num_rows)
    )
    _add_finding(
        ctx,
        Finding(
            "metamorphic",
            placement_name,
            mutated.shape,
            detail,
            mutated.sql,
            mutated.tables,
        ),
    )


def _deep_oracles(ctx, env, gen_query, oracle_table):
    """Run the settings-invariance and TLP oracles on a placement that succeeded."""
    _settings_stage(ctx, env, gen_query, oracle_table)
    _tlp_stage(ctx, env, gen_query)


def _settings_stage(ctx, env, gen_query, oracle_table):
    """Re-run the query under each active variant; every result must still match."""
    if not ctx.force_deep and ctx.rng.random() > 0.5:
        return
    for name, sets, _role in ctx.live_variants:
        table = _run_variant(ctx, env, gen_query.sql, sets)
        ctx.stats.settings_checks += 1
        if table is not None:
            _compare_or_record(
                ctx, table, oracle_table, env_placement(env), name, gen_query
            )


def _run_variant(ctx, env, sql, sets):
    """Apply a variant's SETs, execute, and always RESET ALL afterward."""
    runtime = env.runtime()
    _apply_sets(runtime, sets)
    try:
        return _engine_execute(ctx, env, sql)
    except (
        BaseException
    ):  # noqa: a variant failure is compared as no-table; reset first
        return None
    finally:
        runtime.execute("RESET ALL")


def _apply_sets(runtime, sets):
    """Apply each (setting, value) pair to a live runtime."""
    for setting, value in sets:
        runtime.execute("SET " + setting + " = " + value)


def env_placement(env):
    """Return a short placement label for an environment (its source kinds)."""
    kinds = []
    for source in env.sources:
        kinds.append(source.kind)
    return "+".join(kinds)


def _tlp_stage(ctx, env, gen_query):
    """Run the ternary-logic partitioning check when the shape supports it."""
    spec = gen_query.tlp
    if spec is None:
        return
    predicate = _tlp_predicate(ctx, spec)
    if predicate is None:
        return
    _run_tlp(ctx, env, gen_query, spec, predicate)


def _run_tlp(ctx, env, gen_query, spec, predicate):
    """Execute the whole query and its three predicate partitions and reunite."""
    whole = _tlp_execute(ctx, env, spec, None)
    parts = _tlp_partitions(ctx, env, spec, predicate)
    if whole is None or parts is None:
        return
    ctx.stats.tlp_checks += 1
    _compare_partition_union(ctx, whole, parts, env, gen_query)


def _tlp_partitions(ctx, env, spec, predicate):
    """Execute the P, NOT P, and P IS NULL partitions; None if any fails."""
    tables = []
    for condition in _tlp_conditions(predicate):
        table = _tlp_execute(ctx, env, spec, condition)
        if table is None:
            return None
        tables.append(table)
    return tables


def _tlp_conditions(predicate):
    """Return the three partition conditions of a ternary predicate."""
    return (
        "(" + predicate + ")",
        "NOT (" + predicate + ")",
        "(" + predicate + ") IS NULL",
    )


def _tlp_execute(ctx, env, spec, condition):
    """Build and run one TLP query body (whole when condition is None)."""
    sql = "SELECT " + spec.proj + " FROM " + spec.from_sql
    if condition is not None:
        sql += " WHERE " + condition
    outcome = _execute_attempt(ctx, env, sql)
    return outcome.table


def _compare_partition_union(ctx, whole, parts, env, gen_query):
    """Assert the three partitions, unioned as a multiset, equal the whole."""
    union_rows = _concat_rows(parts)
    whole_rows = _table_rows(whole)
    if sorted(union_rows) != sorted(whole_rows):
        detail = (
            "TLP union rows="
            + str(len(union_rows))
            + " vs whole="
            + str(len(whole_rows))
        )
        _add_finding(
            ctx,
            Finding(
                "tlp", env_placement(env), "-", detail, gen_query.sql, gen_query.tables
            ),
        )


def _concat_rows(tables):
    """Concatenate the string-keyed rows of several tables into one list."""
    rows = []
    for table in tables:
        rows.extend(_table_rows(table))
    return rows


def _table_rows(table):
    """Return a table's rows as sortable string tuples for a multiset compare."""
    rows = []
    for record in table.to_pylist():
        rows.append(_row_key(record))
    return rows


def _row_key(record):
    """Return a stable string key for one row dict (order-independent columns)."""
    parts = []
    for name in sorted(record):
        parts.append(name + "=" + repr(record[name]))
    return "|".join(parts)


def _tlp_predicate(ctx, spec):
    """Pick a predicate column (preferring a non-anchor table) and build P."""
    column = _pick_tlp_column(ctx, spec)
    if column is None:
        return None
    return _build_tlp_predicate(ctx, column[0], column[1])


def _pick_tlp_column(ctx, spec):
    """Choose a predicate (colref, column), biased away from the FROM anchor."""
    foreign = []
    for colref, column in spec.predicate_columns:
        if column.table != spec.anchor_table:
            foreign.append((colref, column))
    pool = foreign if foreign else spec.predicate_columns
    if not pool:
        return None
    return ctx.rng.choice(pool)


def _build_tlp_predicate(ctx, colref, column):
    """Build a simple ternary comparison predicate for a TLP split."""
    from fuzz import generator as generator_mod

    operator = ctx.rng.choice(generator_mod._operators_for(column.category))
    literal = _sample_literal(ctx, column)
    return colref + " " + operator + " " + literal


def _sample_literal(ctx, column):
    """Draw one SQL literal for a column using the generator's literal pools."""
    from fuzz import generator as generator_mod

    if column.category == "str":
        domain = generator_mod.STRING_DOMAINS[(column.table, column.name)]
        return generator_mod._quote_string(ctx.rng.choice(domain))
    return _sample_scalar_literal(ctx, column.category)


def _sample_scalar_literal(ctx, category):
    """Draw one non-string literal from the generator's scalar pools."""
    from fuzz import generator as generator_mod

    pools = {
        "int": generator_mod._INT_POOL,
        "bigint": generator_mod._BIGINT_POOL,
        "decimal": generator_mod._DECIMAL_POOL,
        "double": generator_mod._DOUBLE_POOL,
    }
    if category in pools:
        return str(ctx.rng.choice(pools[category]))
    return _sample_temporal_literal(ctx, category)


def _sample_temporal_literal(ctx, category):
    """Draw a DATE/TIMESTAMP literal for the TLP predicate."""
    from fuzz import generator as generator_mod

    if category == "date":
        return "DATE '" + ctx.rng.choice(generator_mod._DATE_POOL) + "'"
    return "TIMESTAMP '" + ctx.rng.choice(generator_mod._TS_POOL) + "'"


def recheck_reproduces(ctx, sql, tables, order_sensitive, category):
    """Run the full stack on a reduced query; whether it reproduces the category.

    Used by the reducer's predicate: a fresh findings set and forced deep oracles
    over the shared caches, so a candidate that still triggers the same finding
    category is accepted as a valid reduction.
    """
    from fuzz.generator import GenQuery

    sub = _clone_for_recheck(ctx)
    query = GenQuery(sql, tables, "recheck", order_sensitive=order_sensitive)
    try:
        check_query(sub, query)
    except BaseException:  # noqa: a candidate that crashes the stack does not reduce
        return False
    return _has_category(sub.findings, category)


def _clone_for_recheck(ctx):
    """Return a throwaway context sharing caches but with fresh, forced findings."""
    import random

    sub = OracleContext(
        ctx.pg_connection,
        ctx.skip_pg,
        random.Random(0),
        ctx.watchdog_seconds,
        ctx.live_variants,
    )
    sub.env_cache = ctx.env_cache
    sub.oracle_cache = ctx.oracle_cache
    sub.force_deep = True
    return sub


def _has_category(findings, category):
    """Whether any recorded finding has the given category."""
    for finding in findings.values():
        if finding.category == category:
            return True
    return False


def _add_finding(ctx, finding):
    """Record a finding by signature, deduping repeats but counting the first."""
    if finding.signature in ctx.findings:
        return
    ctx.findings[finding.signature] = finding
    ctx.stats.findings += 1


def _sample_placements(ctx, gen_query):
    """Sample the placements to run: the oracle, one pg, one parquet, plus rotation."""
    applicable = _applicable_placements(ctx, gen_query)
    chosen = _required_placements(ctx, applicable)
    _add_rotation(ctx, applicable, chosen)
    return chosen


def _applicable_placements(ctx, gen_query):
    """Return placements whose slot count fits the query and pg availability."""
    result = []
    for placement in PLACEMENTS:
        if len(gen_query.tables) < placement.min_tables:
            continue
        if ctx.skip_pg and _uses_pg(placement):
            continue
        result.append(placement)
    return result


def _uses_pg(placement):
    """Whether a placement has any PostgreSQL slot."""
    return placement_uses_postgres(placement, placement.slots)


def _required_placements(ctx, applicable):
    """Pick the always-included oracle plus one pg and one parquet placement."""
    chosen = []
    _append_named(chosen, applicable, "oracle_single_duck")
    _append_kind(ctx, chosen, applicable, "pg")
    _append_kind(ctx, chosen, applicable, "parquet")
    return chosen


def _append_named(chosen, applicable, name):
    """Append the named placement if applicable and not already chosen."""
    for placement in applicable:
        if placement.name == name and placement not in chosen:
            chosen.append(placement)
            return


def _append_kind(ctx, chosen, applicable, kind):
    """Append one random applicable placement of the given slot kind."""
    pool = []
    for placement in applicable:
        if _has_kind(placement, kind) and placement not in chosen:
            pool.append(placement)
    if pool:
        chosen.append(ctx.rng.choice(pool))


def _has_kind(placement, kind):
    """Whether a placement has a slot of the given kind."""
    for slot in placement.slots:
        if slot.kind == kind:
            return True
    return False


def _add_rotation(ctx, applicable, chosen):
    """Add up to two more applicable placements to widen per-iteration coverage."""
    pool = []
    for placement in applicable:
        if placement not in chosen:
            pool.append(placement)
    ctx.rng.shuffle(pool)
    for placement in pool[:2]:
        chosen.append(placement)


class VariantReport:
    """A variant's classification: live (changes a plan), control, or dead."""

    def __init__(self, name, role, status, evidence):
        """Store the variant name, role, live/dead/control status, and evidence."""
        self.name = name
        self.role = role
        self.status = status
        self.evidence = evidence


def verify_variants(pg_connection, skip_pg):
    """Classify every VARIANT by whether it changes a probe query's EXPLAIN plan.

    Returns (live_variants, reports): ``live_variants`` are the (name, sets, role)
    tuples the settings oracle should sweep - every variant whose SET changes at
    least one probe plan, plus the control - and ``reports`` explains each,
    including the dead knobs that never changed a plan.
    """
    placements = _probe_placements(skip_pg)
    envs = _probe_environments(placements, pg_connection)
    reports = _classify_variants(envs)
    live = _live_variant_defs(reports)
    return live, reports


def _probe_placements(skip_pg):
    """Return the placements used for plan probing (pg-bearing unless pg is off)."""
    names = ("duck_duck",) if skip_pg else _PROBE_PLACEMENTS
    chosen = []
    for placement in PLACEMENTS:
        if placement.name in names:
            chosen.append(placement)
    return chosen


def _probe_environments(placements, pg_connection):
    """Build one engine environment per (placement, probe query) for comparison."""
    envs = []
    for placement in placements:
        _append_probe_envs(placement, pg_connection, envs)
    return envs


def _append_probe_envs(placement, pg_connection, envs):
    """Append the probe environments for one placement to the env list."""
    for sql, tables in PROBE_QUERIES:
        specs = case_table_specs({"name": "probe", "tables": tables, "query": sql})
        env = runtime_mod.build_environment(placement, specs, pg_connection)
        envs.append((env, sql))


def _classify_variants(envs):
    """Return a VariantReport for every declared variant."""
    reports = []
    for name, sets, role in VARIANTS:
        reports.append(_classify_one_variant(name, sets, role, envs))
    return reports


def _classify_one_variant(name, sets, role, envs):
    """Classify one variant as control, live, or dead against the probes."""
    if role == "control":
        return VariantReport(
            name, role, "control", "budget raise; no plan change expected"
        )
    changed = _variant_changes_a_plan(sets, envs)
    if changed is not None:
        return VariantReport(name, role, "live", "changed plan of probe: " + changed)
    return VariantReport(name, role, "dead", "no probe plan changed")


def _variant_changes_a_plan(sets, envs):
    """Return the first probe SQL whose plan changes under the SETs, or None."""
    for env, sql in envs:
        if _plan_changes(env, sql, sets):
            return sql[:48]
    return None


def _plan_changes(env, sql, sets):
    """Whether a variant's SETs change one probe query's EXPLAIN plan text."""
    rendered = env.render(sql)
    base = _explain(env, rendered)
    _apply_sets(env.runtime(), sets)
    try:
        changed = _explain(env, rendered)
    finally:
        env.runtime().execute("RESET ALL")
    return base != changed


def _explain(env, rendered_sql):
    """Return the engine's EXPLAIN plan text for a rendered query."""
    import pyarrow as pa

    table = pa.table(env.runtime().execute("EXPLAIN " + rendered_sql))
    return "\n".join(table.column("plan").to_pylist())


def _live_variant_defs(reports):
    """Return the (name, sets, role) defs for live and control variants."""
    keep = []
    for name, sets, role in VARIANTS:
        if _report_is_active(reports, name):
            keep.append((name, sets, role))
    return keep


def _report_is_active(reports, name):
    """Whether a variant's report marks it live or control (not dead)."""
    for report in reports:
        if report.name == name:
            return report.status in ("live", "control")
    return False
