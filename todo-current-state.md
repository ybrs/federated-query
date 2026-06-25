# Current State - federated-query

ASCII only. No emoji, glyphs, or extended characters in this file.

## Project state

- Phase 8 was merged to `main`. This session's work is on a branch the user
  manages. Git was not used by the assistant per instruction.
- Test suite last run: 811 passed, 3 skipped. The 3 skips are ClickHouse
  connector tests that skip when no ClickHouse server is reachable
  (environmental, not failures). black is clean on all touched files.
- Run tests: `make pg-start` then
  `POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python -m pytest tests/ -q`.

## Completed this session

1. Doc and roadmap restructure in `tasks.md`. Old handoff docs merged in.
   Phases renumbered so numeric order equals execution order:
   - Phase 9:  Window functions and remaining SQL breadth
   - Phase 10: General dependent-join decorrelation + cross-source correlated
     fallback (cluster D)
   - Phase 11: Cost-based optimization
   - Phase 12: Advanced execution
   - Phase 13: Production readiness
   - Phase 14: Advanced features

2. No-emoji rule added to `CLAUDE.md` and `AGENTS.md`, plus a memory entry.
   About 480 status glyphs were stripped from the repo earlier in the session.

3. Docstrings added to about 50 substantive undocumented functions
   (cli/fedq.py, optimizer, binder, planner).

4. Phase 9 (window functions) COMPLETE:
   - Section 9.3: new `WindowExpr` expression node; parser and binder support;
     single-source windows push as one remote query; cross-source
     `PhysicalWindow` runs in the DuckDB merge engine with alias-aware column
     resolution.
   - Section 9.4: correlated-window decorrelation. Lifts correlation columns
     into the window's PARTITION BY, reuses the existing pick-one GroupedLimit
     and LEFT-join path. Non-equi correlation fails fast.
   - Tests added: `tests/e2e_pushdown/test_window_functions.py` (single and
     cross-source) and `tests/e2e_decorrelation/test_window_subqueries.py`
     (deterministic, bounds, fail-fast).

## Open work: SQL audit (verified by execution, NOT yet fixed)

Root cause: the parser silently ignores clauses it does not handle, which can
return wrong answers. This violates the "never fail silently" rule.

Silent wrong answers (highest priority):
- `DISTINCT ON (...)`        -> drops the ON, returns all rows.
- named `WINDOW w AS (...)`  -> drops the definition, window computed over the
                               wrong frame.
- `TABLESAMPLE`             -> ignored, returns the full table.

Crash or opaque error (should be a clean fail-fast):
- `GROUP BY ROLLUP / CUBE / GROUPING SETS` -> opaque source BinderException.
- `FETCH FIRST ... WITH TIES`              -> internal AttributeError crash.

Already clean or supported:
- `UNNEST`, `WITHIN GROUP` (ordered-set aggregates) -> clear "unsupported" error.
- `QUALIFY`, plain `DISTINCT`, window frames (`ROWS BETWEEN ...`),
  `LAG`/`LEAD`/`RANK`/`NTILE`, `LIMIT`/`OFFSET` -> work correctly.

Recommended next step: a parser safety sweep that fails fast on any clause it
cannot faithfully handle (fixes the silent wrong-answer bugs), then optionally
implement `ROLLUP`/`CUBE`/`GROUPING SETS` and `DISTINCT ON` (both render
natively to the source and the merge engine, like windows did).

## Where context lives

- `tasks.md`            - roadmap, phase details, architecture appendix.
- memory `federated-query-status` - detailed running state including Phase 9
  work and the SQL audit findings.
- The SQL audit details above are not yet written into `tasks.md`.
