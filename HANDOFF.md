# Handoff: test harness rebuild and the plan-equivalence defect

Branch `test-harness-fixes`. Written 2026-08-01.

Note: `CLAUDE.md` states that work tracking lives in taskq and that no checked-in
handoff document is to be created; `HANDOFF.md` was deleted in commit a77a7c3 for
that reason. This file exists because the maintainer asked for it. Everything in
it that is durable is also in taskq (tasks 276-281); if the two disagree, taskq
is right.

## What was committed

Three commits, each gate-green when made:

- `f2471ca` Test data is built once by one script, and the suite only reads it
- `02e7738` Engine configs are built with the data, and a run starts from clean engine state
- `d05e7bf` A placement is a name in the SQL, not a set of files

The suite used to create its own data as it ran. It re-seeded 965 times per run
where only 290 environments existed, held one engine runtime per environment
alive (374 PostgreSQL connections), and dropped every `fed_%` schema at teardown
so a second consecutive run collapsed with 1701 failures against data the first
run had deleted.

Now `build_test_harness.sh` is the only thing that creates test data, and
`run_all_tests.sh` verifies it per table and per row count and dies naming what
is missing. The corpus test path performs no file writes at all.

The last commit is the one that matters most. A placement - which datasource
each table is read from - used to be baked into the data layout, one set of
sources per placement per table set: 548 datasources and 290 configs. It does
not need to be. The engine decides pushdown from which datasource each
*referenced* table belongs to, so if every source holds every table, a placement
is just a choice of names in the SQL:

    SELECT ... FROM pg1.fed_pg1.orders JOIN duck1.main.customers ...
    SELECT ... FROM duck1.main.orders  JOIN pg1.fed_pg1.customers ...

The harness is now 6 instances of each kind, each holding every table, and one
config: **18 datasources, 1 config, built in 1.7s** (was 548 and 290 in 12.2s).

## What is uncommitted

Working tree on `test-harness-fixes`:

- `tests/e2e_federated/harness.py` - `variations()`, the distribution enumerator
- `tests/e2e_federated/conftest.py` - parametrizes over variations, no skip path,
  `pytest_sessionfinish` fails any run containing a skip
- `tests/e2e_federated/test_corpus.py` - skip path deleted
- `tests/e2e_federated/known_failures.py` - xfail registry (written before the
  numbers below were established; needs revisiting)
- `tests/e2e_federated/run_isolated.py`, `run_isolated_tests.sh` - the isolated
  runner, now obsolete (see below)
- `crates/` - the `optimizer.enable_learned_statistics` patch, 3-way applied from
  an agent worktree branch based on `ceff6c7`. **Not yet verified by the gate on
  this branch:** `cargo test --workspace`, clippy and fmt have not been run with
  it here.

## The corpus is now enumerated

Every case runs under every distribution of its tables: every partition into
co-located groups, each group on DuckDB, PostgreSQL or Parquet. Validated
against the Stirling numbers for n=1..6. **86,520 variations**, up from 4,545.

There is no skip path. A distribution a case does not have is never generated.
The four selective switches that existed (`FEDQ_E2E_CORPUS`, `FEDQ_E2E_SKIP_PG`,
`make test-no-db`, and two modules that skipped themselves when a binary was
unbuilt) are deleted.

## The numbers

Whole corpus, one process, reproducible:

    learned statistics OFF : 20 failures   8.2 min   (2 runs, identical)
    learned statistics ON  : 14 failures   8.8 min   (3 runs, identical)

**20 is the real number.** The six-failure gap is listed in task 278.

## What the enumeration found

Two engine defects, in shapes the old fixed placement list never generated:

- **Task 277** (urgent/critical) - `INTERSECT ALL` and `EXCEPT ALL` pushed whole
  into one Parquet source return the wrong row counts. INTERSECT ALL one too
  many, EXCEPT ALL one too few. **Wrong rows, no error.** DuckDB run over the
  same Parquet files with the same SQL is correct, so the engine is at fault.
  5 failures, all the `pq1-pq1` shape.
- **Task 278** (high/major) - a derived table's column alias is dropped when it
  is pushed down co-located with the table it joins to, so the outer join on the
  alias raises `No field named ik`. 15 failures.

Both are held `blocked` pending the fix for the defect below.

## The defect that matters most

**Task 280: we generate wrong plans.** The optimizer produces plans that are not
result-equivalent. Same query, same data - one plan is correct, another is wrong.

Proved by a one-row experiment: inserting a single `measured_ndv` row into the
live runtime, with no restart, switches which plan is chosen and takes a query
from FAIL to PASS.

Statistics are not the defect - picking between plans is their job, and join
ordering depends on them. One of the plans they pick between is broken. And
task 277 shows a wrong plan does not always announce itself.

**Nothing checks that plans agree.** Each of the 86,520 variations runs exactly
one plan, whichever was chosen under whatever statistics existed at that moment.
Coverage of the SQL surface is broad; coverage of the plan space is one sample
per query. The fuzzer's settings-invariance oracle is the right shape of check
but runs only over fuzz-generated queries and a fixed toggle list.

## Also open

- **Task 276** - the engine derives its state location (learned-stats catalog,
  MV store) from the config file's path, with no way to configure it. The suite
  works around it by clearing that state at session start.
- **Task 281** - opening one DuckDB datasource costs the engine 167ms where
  DuckDB itself needs 18ms for the same work. `Runtime()` over the 18-source
  config takes 1129ms; a query takes 4ms.

## Things that will bite you

- **The pre-commit hook used to test with the wrong interpreter.** It preferred
  `$VIRTUAL_ENV` over the repository venv, so commits were verified by whatever
  the developer had activated. Fixed to use `./venv` unconditionally. There are
  four virtualenvs on the dev machine; only `./venv` is the right one.
- **The isolated runner is obsolete.** It gave each variation its own process and
  engine to escape the learned-statistics dependence. `enable_learned_statistics
  = false` removes that dependence directly, so one shared process is as
  trustworthy and takes 8 minutes instead of 33 hours. Its results file
  (`isolated-results.jsonl`, 34,540 records) is a partial run and contains
  artifacts: budget kills caused by running 10 workers on 12 cores, and records
  from harness clones deleted mid-run. Do not read a failure count out of it.
- **The planning budget is wall clock**, so anything else loading the machine can
  push a plan past it and manufacture failures. Measure on an idle machine.
