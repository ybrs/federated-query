# Federated Query Engine - Implementation Tasks

## Overview

This document breaks down the implementation into phases. Each phase builds on the previous one, aiming for a working end-to-end system early, then adding optimizations and advanced features.

## Phase Roadmap Summary

| Phase | Status | Description | Tests |
|-------|--------|-------------|-------|
| **Phase 0** | Complete | Foundation and infrastructure | 50 tests |
| **Phase 1** | Complete | Basic query execution (single-table SELECT) | 19 tests |
| **Phase 2** | Complete | Joins and multi-table queries | 5 tests |
| **Phase 3** | Complete | Aggregations and GROUP BY | 9 tests |
| **Phase 4** | Complete | Pre-optimization and expression handling | 42 tests |
| **Phase 5** | Complete | Statistics and cost model | 30 tests |
| **Phase 6** | Substantially Complete | Logical optimization (predicate/projection/limit pushdown, column pruning); join reordering deferred to Phase 10 | 48 tests |
| **Phase 7** | Complete | Decorrelation (subqueries) → SEMI/ANTI/LEFT/LATERAL joins | green |
| **Phase 8** | Complete | Pushdown breadth, merge engine, set ops, dynamic filtering, LATERAL, CTEs (single + cross-source), date/time + FILTER + NATURAL/USING | 809 passing |
| **Phase 9** | Not Started | General dependent-join decorrelation + cross-source correlated-subquery fallback (cluster D) — the unnest machinery + tests, **before** the cost work | - |
| **Phase 10** | Not Started | Cost-based optimization: cost-driven physical-plan selection, join reordering, broadcast join | - |
| **Phase 11** | Not Started | Advanced execution: parallel fetch/join, memory mgmt + spill-to-disk, streaming/pipelining | - |
| **Phase 12** | Not Started | Window functions + remaining SQL breadth | - |
| **Phase 13** | Not Started | Production readiness (structured error codes, observability, tuning, benchmarks) | - |
| **Phase 14** | Future | Advanced features (adaptive execution, result caching, more sources, write ops) | - |

**Current Status**: Phases 0–8 complete (branch `phase8`), full suite **809 passing / 0 failed / 0 xfailed** (re-verified 2026-06-25 against live Postgres). The live handoff, pushdown-capability matrix, and decorrelation-gap notes that used to live in `TODO-next.md` / `pushdown-status.md` / `decorrelation-gaps.md` are now merged into this file — see Phase 8 (delivered), Phase 9 (remaining decorrelation gaps), and the **Architecture Quick Map & Current Capabilities** appendix at the end.

> **Phase renumber note (2026-06-25):** what shipped under the "Phase 8" label was pushdown breadth + decorrelation + CTEs — *not* the original §8.1–8.5 "cost-based physical planning" plan. Cost-based plan selection turned out to be a phase of its own and now lives in **Phase 10** (with join reordering, which depends on it). The general dependent-join unnest machinery (old §9.6) is pulled forward to **Phase 9** because its cases and tests are prerequisites for cost-based planning. The old Phase 9 (parallel exec / memory) → Phase 11; old Phase 10 (SQL features) → Phase 12; old Phase 11/12 → Phase 13/14.

## Phase 0: Foundation COMPLETED

**Goal**: Set up project structure and basic infrastructure

- [x] Create project structure and directories
- [x] Set up dependencies (sqlglot, psycopg2, duckdb, pyarrow)
- [x] Create configuration system (YAML-based)
- [x] Implement data source catalog
- [x] Create basic logging and error handling
- [x] Set up testing framework (pytest)
- [x] Create development environment (Docker compose with PostgreSQL)

**Deliverable**: Project skeleton with configuration loading

**Notes**:
- All 50 tests passing
- PostgreSQL and DuckDB connectors fully implemented with connection pooling
- Catalog supports metadata discovery and table resolution
- Structured logging with JSON formatter available
- Skeleton classes created for parser, binder, executor, and plan nodes

---

## Phase 1: Basic Query Execution COMPLETED

**Goal**: Execute simple single-table queries on remote sources

**Status**: Fully implemented and tested

**Implementation Summary**:

All Phase 1 components have been successfully implemented with comprehensive test coverage.
The system can now execute queries like: `SELECT col1, col2 FROM datasource.table WHERE col1 > 10 LIMIT 100`

### 1.1 Parser and AST Conversion (done)
- [x] Integrate sqlglot parser (can parse SQL to AST)
- [x] Create logical plan node base classes (Scan, Projection, Filter, Limit)
- [x] Implement AST to logical plan converter (parser.py:ast_to_logical_plan)
  - [x] Handle SELECT statements
  - [x] Handle FROM clauses (create Scan nodes)
  - [x] Handle WHERE clauses (create Filter nodes)
  - [x] Handle column selections (create Projection nodes)
  - [x] Handle LIMIT clauses
- [x] Test parser with various simple queries (4 tests passing)

### 1.2 Catalog and Binder (done)
- [x] Implement catalog interface (Catalog class works)
- [x] Create schema metadata classes (Table, Column, Type)
- [x] Implement binder to resolve table/column references (binder.py:bind)
  - [x] Resolve table references using catalog
  - [x] Resolve column references
  - [x] Validate column existence
  - [x] Add data type information to expressions
- [x] Support multi-schema (datasource.schema.table)
- [x] Test binding with various table/column references (8 tests passing)

### 1.3 Data Source Connectors (done)
- [x] Create DataSource abstract interface
- [x] Implement PostgreSQL connector
  - [x] Connection pooling
  - [x] Query execution
  - [x] Result fetching as Arrow tables
  - [x] Metadata discovery (tables, columns, types)
- [x] Implement DuckDB connector
  - [x] Database connection
  - [x] Query execution
  - [x] Result fetching as Arrow tables
  - [x] Metadata discovery

### 1.4 Basic Executor (done)
- [x] Create physical plan node base classes
- [x] Implement physical scan operator (PhysicalScan.execute())
  - [x] Generate SQL from scan parameters
  - [x] Execute on remote data source
  - [x] Stream results as Arrow batches
- [x] Implement physical filter operator (PhysicalFilter.execute())
  - [x] Evaluate filter expressions locally using PyArrow compute
  - [x] Filter Arrow batches
  - [x] Support comparison operators (=, !=, <, <=, >, >=)
  - [x] Support logical operators (AND, OR)
- [x] Implement physical projection operator (PhysicalProjection.execute())
  - [x] Perform column projection on input batches
  - [x] Handle column references
- [x] Implement physical limit operator (PhysicalLimit.execute())
  - [x] Limit output rows
  - [x] Handle offset correctly
- [x] Create simple physical planner (logical to physical, 1:1 mapping)
- [x] Executor infrastructure (calls plan.execute())

### 1.5 Testing (done)
- [x] Test single-table SELECT queries (7 end-to-end tests passing)
- [x] Test WHERE clause execution
- [x] Test column projection
- [x] Test LIMIT execution
- [x] Test end-to-end: SQL -> logical plan -> physical plan -> execution -> results

**Deliverable**: Fully functional - can execute `SELECT col1, col2 FROM datasource.table WHERE col1 > 10 LIMIT 100`

**Implementation Notes**:
- All physical operators stream results as Arrow RecordBatches for memory efficiency
- Filter evaluation uses PyArrow compute functions for performance
- Literal type inference automatically converts string literals to proper types (int, float, bool)
- ColumnRef expressions now track resolved data types after binding
- Physical planner injects datasource connections into scan nodes
- All tests use in-memory DuckDB for fast execution

**Test Coverage**:
- Parser: 4/4 tests passing
- Binder: 8/8 tests passing
- End-to-end: 7/7 tests passing
- **Total: 19 tests passing**

**Known Limitations** (to be addressed in future phases):
- WHERE clause filters are evaluated locally (no pushdown optimization yet)
- Only simple column projections supported (no complex expressions)
- No join support yet
- No aggregation support yet
- Cost estimation not implemented

---

## Phase 2: Joins and Multi-Table Queries

**Status:** COMPLETED
**Goal**: Execute joins across data sources
**Tests**: 5 join tests passing, 70 total tests
**Completion Date**: Phase 2 completed

### 2.1 Logical Plan - Joins (done)
- [x] Add Join logical plan node
- [x] Support join types (INNER, LEFT, RIGHT, FULL, CROSS)
- [x] Parse ON conditions and extract join predicates
- [x] Handle multi-way joins

### 2.2 Physical Join Operators (done)
- [x] Implement Hash Join
  - [x] Build hash table from right side
  - [x] Probe with left side
  - [x] Handle NULL values correctly (basic INNER JOIN)
  - [x] Schema merging with duplicate column renaming
- [x] Implement Nested Loop Join (fallback for non-equi joins)
- [x] Implement cross-product handling
- [x] Schema resolution for joins

### 2.3 Join Strategy Selection (Basic) (done)
- [x] Choose join operator based on join type (Hash vs Nested Loop)
- [x] Extract equi-join keys from conditions
- [x] Physical planning for joins

**Note**: Advanced join optimizations (remote join pushdown, data gathering) deferred:
- Data gathering and parallel fetching → **Phase 9, section 9.1**
- Join pushdown to same datasource → **Phase 6, section 6.7**

### 2.4 Testing (done)
- [x] Test joins on same data source
- [x] Test JOIN with WHERE clauses
- [x] Test specific column selection in joins
- [x] Test SELECT * with joins
- [x] Verify logical plan creation for joins
- [x] Test federated join across PostgreSQL and DuckDB (example/query.py)

**Deliverable**: Can execute `SELECT c.name, o.order_id, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > 100`

**Implementation Summary**:
- **Parser (federated_query/parser/parser.py:395-450)**: Converts sqlglot JOIN AST to logical Join nodes, extracts join conditions
- **Binder (federated_query/parser/binder.py:175-220)**: Handles multi-table column resolution with table aliases and scope management
- **PhysicalHashJoin (federated_query/plan/physical.py:263-412)**: In-memory hash join with configurable build side, handles equi-joins efficiently
- **PhysicalNestedLoopJoin (federated_query/plan/physical.py:415-577)**: Fallback for non-equi joins and complex conditions
- **Physical Planner (federated_query/optimizer/physical_planner.py:50-110)**: Chooses join strategy based on condition analysis (equi-join → hash join, non-equi → nested loop)
- **Test Coverage**:
  - tests/test_e2e_joins.py: 5 comprehensive join tests
  - example/query.py: Real-world federated join example
- All joins currently execute locally by materializing data from sources (cross-datasource parallel gathering deferred to Phase 9)

---

## Phase 3: Aggregations and Grouping COMPLETED

**Status:** FULLY COMPLETED
**Goal**: Support GROUP BY and aggregation functions
**Tests**: 9/9 aggregation tests passing, 79 total tests

### 3.1 Logical Plan - Aggregations (done)
- [x] Add Aggregate logical plan node
- [x] Parse GROUP BY clauses (single and multiple columns)
- [x] Parse aggregate functions (SUM, COUNT, AVG, MIN, MAX)
- [x] Handle HAVING clauses with expression rewriting

### 3.2 Physical Aggregate Operators
- [x] Implement Hash Aggregate (PhysicalHashAggregate)
- [x] Support grouping by multiple columns
- [x] Implement aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- [x] Handle NULL values in aggregations
- [x] Support global aggregations (without GROUP BY)
- [x] COUNT(*) support
- [x] Schema inference for aggregate results

### 3.3 Testing (done)
- [x] Test simple aggregations (COUNT, SUM, AVG)
- [x] Test GROUP BY with multiple columns
- [x] Test global aggregations (without GROUP BY)
- [x] Test multiple aggregates in single query
- [x] Test federated JOIN + aggregation
- [x] Test HAVING clause evaluation (both COUNT and SUM)

**Note**: Aggregate pushdown optimization deferred to **Phase 6, section 6.8**

**Deliverable**: Can execute `SELECT region, COUNT(*), AVG(amount) FROM orders GROUP BY region HAVING COUNT(*) > 2`

**Implementation Notes**:
- Parser converts sqlglot AggFunc nodes to FunctionCall expressions
- PhysicalHashAggregate uses hash table with accumulator pattern
- Binder resolves aggregate expressions with proper type inference
- Physical planner converts Aggregate logical nodes to PhysicalHashAggregate
- All aggregate functions handle NULL values correctly
- Supports federated aggregations over JOINs across PostgreSQL and DuckDB
- **HAVING clause** support with expression rewriting (parser rewrites aggregate functions to column references)
- Binder handles HAVING predicates referencing aggregate output columns
- Comprehensive examples in `example/aggregate_queries.py` and `example/query.py`
- Documentation in `PHASE_3_COMPLETION.md` and `FEDERATED_AGGREGATION_EXAMPLES.md`

**Implementation Details - HAVING Clause**:
- Parser rewrites HAVING expressions to reference aggregate output columns
- Expression rewriter maps aggregate functions (e.g., `COUNT(*)`) to output column names (e.g., `order_count`)
- Binder validates HAVING predicates against aggregate output schema
- Filter node applies after aggregation completes

**Future Enhancements** (deferred to later phases):
- Aggregation pushdown to source databases → **Phase 6**
- Partial aggregation for distributed execution → **Phase 6**
- COUNT(DISTINCT) support → **Phase 10**
- Advanced aggregates (STDDEV, VARIANCE, PERCENTILE, etc.) → **Phase 10**

---

## Phase 4: Pre-Optimization and Expression Handling COMPLETED

**Status:** FULLY COMPLETED
**Goal**: Simplify expressions and perform basic optimizations
**Tests**: 42 new tests (33 expression rewriter + 9 optimization rules), 121 total tests passing

### 4.1 Expression System (done)
- [x] Create expression node classes
  - [x] Literals, Column references
  - [x] Binary/Unary operators
  - [x] Function calls
  - [x] CASE expressions
- [x] Implement expression evaluator (via expression rewriter)
- [x] Type inference for expressions

### 4.2 Pre-Optimization Rules (done)
- [x] Constant folding (`1 + 2` → `3`)
- [x] Expression simplification
  - [x] `x AND TRUE` → `x`
  - [x] `x OR FALSE` → `x`
  - [x] `NOT (NOT x)` → `x`
  - [x] `x + 0` → `x`, `x * 1` → `x`, `x - x` → `0`
  - [x] `x * 0` → `0`, `x / 1` → `x`
  - [x] Double negation elimination
- [x] Arithmetic simplification
- [x] Null handling simplification (IS NULL, IS NOT NULL)

### 4.3 Testing (done)
- [x] Test constant folding (18 tests)
- [x] Test expression simplification (13 tests)
- [x] Test composite rewriter (2 tests)
- [x] Test optimization rule on logical plans (9 tests)
- [x] Test complex WHERE clauses

**Deliverable**: Queries with complex expressions are simplified before execution

**Implementation Summary**:
- **ExpressionRewriter** (federated_query/optimizer/expression_rewriter.py): Base class and visitor pattern for expression transformation
- **ConstantFoldingRewriter**: Evaluates constant expressions at compile time (arithmetic, comparison, logical, unary operations)
- **ExpressionSimplificationRewriter**: Applies algebraic simplification rules (identity elements, absorption laws, double negation)
- **CompositeExpressionRewriter**: Chains multiple rewriters in sequence for multi-pass optimization
- **ExpressionSimplificationRule** (federated_query/optimizer/rules.py): Logical plan optimization rule that applies expression rewriting to all expressions in a plan tree
- Comprehensive test coverage in tests/test_expression_rewriter.py and tests/test_optimization_rules.py
- All optimizations preserve expression semantics and handle NULL values correctly

**Key Features**:
- Constant folding supports all binary operators (arithmetic, comparison, logical) and unary operators (NOT, negation, IS NULL)
- Expression simplification includes identity laws (x AND TRUE → x), zero laws (x * 0 → 0), and inverse laws (x - x → 0)
- Safe handling of division by zero (not folded) and NULL values
- Recursive rewriting for nested expressions
- Integration with logical plan optimizer framework
- Zero-cost abstraction: rewriters only create new nodes when transformations occur

**Future Enhancements** (deferred to later phases):
- Predicate normalization (CNF/DNF conversion) - deferred to Phase 6
- More advanced simplifications (De Morgan's laws, distributive law)
- Expression canonicalization for common subexpression elimination
- Cost-based expression rewriting decisions

---

## Phase 5: Statistics and Cost Model COMPLETED

**Status:** FULLY COMPLETED
**Goal**: Implement statistics collection and cost estimation
**Tests**: 30 comprehensive cost model tests, 154 total tests passing

**Prerequisites**: Statistics infrastructure already exists:
- TableStatistics and ColumnStatistics data classes (federated_query/datasources/base.py)
- StatisticsCollector class with caching (federated_query/optimizer/statistics.py)
- Both PostgreSQL and DuckDB implement get_table_statistics()
- CostConfig with cost parameters (federated_query/config/config.py)
- CostModel fully implemented (federated_query/optimizer/cost.py)

### 5.1 Statistics Collection (done)
- [x] Define statistics schema (TableStats, ColumnStats) - Already implemented
- [x] Implement statistics collector infrastructure - Already implemented
  - [x] Row counts - Implemented in datasources
  - [x] Column cardinality (NDV) - Implemented in datasources
  - [x] Null fractions - Implemented in datasources
  - [x] Data size estimation - Basic implementation exists
- [x] Statistics caching with TTL - Basic caching implemented

**Deferred to Phase 6+:**
- Sampling for large tables
- Histogram collection for key columns
- Most common values tracking

### 5.2 Cost Model Implementation (done)
- [x] Define cost model parameters (CPU, IO, Network costs) - Already in CostConfig
- [x] Implement cost estimation for each operator
  - [x] Scan cost (IO + CPU + network costs)
  - [x] Filter cost (input cost + CPU processing)
  - [x] Join cost (build + probe costs)
  - [x] Aggregate cost (hash build + finalize)
  - [x] Projection cost (input cost + expression eval)
  - [x] Limit cost (minimal CPU cost)
- [x] Implement cardinality estimation
  - [x] Base table cardinality (from statistics)
  - [x] Filter selectivity estimation
  - [x] Join selectivity estimation
  - [x] Aggregate cardinality estimation
  - [x] Independence assumption for multiple predicates

### 5.3 Selectivity Estimation (done)
- [x] Implement selectivity for comparison operators
  - [x] Equality: `1 / num_distinct` (with bounds checking)
  - [x] Inequality (<, >, <=, >=): heuristic (default 0.33)
  - [x] LIKE patterns: heuristic (default 0.1)
  - [x] IS NULL / IS NOT NULL: uses null_fraction from statistics
- [x] Implement selectivity for logical operators
  - [x] AND: product of operand selectivities
  - [x] OR: `1 - product of (1 - operand selectivities)`
  - [x] NOT: `1 - operand selectivity`
- [x] Handle edge cases (division by zero, missing statistics)

**Deferred to Phase 6+:**
- Histogram-based selectivity estimation

### 5.4 Integration with Optimizer
- [x] Basic physical plan cost estimation implemented
- [ ] Full integration with physical planner - DEFERRED to Phase 8
- [ ] Use cardinality estimates for join ordering - DEFERRED to Phase 6
- [ ] Cost-based join strategy selection - DEFERRED to Phase 8
- [ ] Annotate physical plans with costs - DEFERRED to Phase 8

### 5.5 Testing (done)
- [x] Test statistics collection from both PostgreSQL and DuckDB
- [x] Test statistics caching and refresh logic
- [x] Verify cost estimation for simple queries (scan, filter, project)
- [x] Verify cost estimation for joins (hash join)
- [x] Verify cost estimation for aggregations
- [x] Test cardinality estimation for all operator types
- [x] Test selectivity estimation for various predicates
- [x] Test cost increases with cardinality
- [x] Test complex multi-operator plans

**Deliverable**: Fully functional cost model with cardinality and selectivity estimation

**Implementation Summary**:
- **Cardinality Estimation** (federated_query/optimizer/cost.py:48-161):
  - Scan: Uses table statistics or defaults to 1000
  - Filter: Applies selectivity to input cardinality
  - Projection: Same as input (no row reduction)
  - Join: Type-aware estimation (INNER, LEFT, RIGHT, FULL, CROSS)
  - Aggregate: 1 for global, estimated groups for GROUP BY
  - Limit: min(input_card, offset + limit)

- **Selectivity Estimation** (federated_query/optimizer/cost.py:163-293):
  - Equality: 1/num_distinct from column statistics
  - Inequality: 0.33 default heuristic
  - AND: product of operand selectivities
  - OR: 1 - product of (1 - operand selectivities)
  - NOT: 1 - operand selectivity
  - IS NULL/IS NOT NULL: uses null_fraction

- **Cost Estimation** (federated_query/optimizer/cost.py:295-415):
  - Logical plan costs: Recursive estimation with operator-specific formulas
  - Physical plan costs: Skeleton implementation for future use
  - Cost components: CPU, IO, and network costs
  - Scan cost considers IO, CPU, and network transfer
  - Join cost includes build and probe phases
  - Aggregate cost includes hash table build and finalization

**Test Coverage** (tests/test_cost_model.py):
- 10 selectivity estimation tests
- 10 cardinality estimation tests
- 10 operator cost estimation tests
- All tests passing with proper assertions

**Future Enhancements** (deferred to later phases):
- Histogram-based selectivity → **Phase 6**
- Sampling for large tables → **Phase 6**
- Most common values tracking → **Phase 6**
- Full physical planner integration → **Phase 8**
- Cost-based join strategy selection → **Phase 8**

**Current Status**: 154 tests passing, Phase 5 complete

---

## Phase 6: Logical Optimization

**Status:** SUBSTANTIALLY COMPLETE
**Goal**: Implement rule-based logical optimizations
**Tests**: 32 tests passing, 186 total tests

**Prerequisites**: Optimization infrastructure already exists:
- OptimizationRule base class (federated_query/optimizer/rules.py)
- RuleBasedOptimizer with rule application engine
- ExpressionSimplificationRule (fully implemented in Phase 4)
- Skeleton rules: PredicatePushdownRule, ProjectionPushdownRule, JoinReorderingRule

### 6.1 Optimization Framework (done)
- [x] Create optimization rule interface - Already implemented
- [x] Implement rule application engine - Already implemented (RuleBasedOptimizer)
- [x] Support multiple optimization passes with different rule sets - Implemented
- [x] Add rule ordering and fixed-point iteration for cascading optimizations - Implemented

### 6.2 Predicate Pushdown (done)
- [x] Push filters through projections - Implemented
- [x] Push filters to data sources (integrate with Scan node) - Implemented
- [x] Merge adjacent filters - Implemented
- [x] Push filters below joins - Implemented
  - [x] Split predicates by join side (left-only, right-only, both) - Implemented
  - [x] Push left-only predicates to left side - Implemented
  - [x] Push right-only predicates to right side - Implemented
  - [x] Keep predicates referencing both sides above join - Implemented
  - [x] Preserve semantics for outer joins (don't push below null-generating side) - Implemented
- [ ] Split complex predicates (break up AND conjunctions) - Deferred
- [ ] Convert filter expressions to SQL - Deferred to execution
- [ ] Handle datasource capabilities - Deferred to execution

### 6.3 Projection Pushdown (done)
- [x] Column analysis infrastructure - Implemented
  - [x] Analyze column usage throughout plan tree
  - [x] Extract columns from expressions (binary, unary, **function calls**)
- [x] Column pruning (remove unused columns early) - Implemented
  - [x] Remove unused columns from Scan nodes - Implemented
  - [x] Propagate required columns through filters - Implemented
  - [x] Handle join column requirements - Implemented
- [ ] Push projections to data sources (SELECT only needed columns) - Deferred to execution
- [ ] Eliminate redundant projections - Deferred

### 6.4 Join Reordering (pending)
- [ ] Implement dynamic programming for small join graphs (< 10 tables)
  - [ ] Build all valid join trees
  - [ ] Use cost model to select best tree
  - [ ] Memoize intermediate results
- [ ] Implement greedy heuristic for large join graphs (>= 10 tables)
  - [ ] Iteratively join smallest intermediate results
  - [ ] Use cardinality estimates from Phase 5
- [ ] Consider selectivity and cardinality from cost model
- [ ] Preserve join semantics (left/right outer joins must maintain order)
- [ ] Handle cross joins (Cartesian products)

**Note**: Join reordering requires Phase 5 cost model integration → moved to **Phase 10** (Cost-Based Optimization)

### 6.5 Limit Pushdown (done)
- [x] Push LIMIT through projections - Implemented
- [x] **FIXED**: Do NOT push LIMIT through filters (changes semantics) - Implemented
- [x] Handle LIMIT with offset - Implemented
- [x] Cannot push LIMIT through joins (in general) - Implemented
- [x] Cannot push LIMIT through aggregations - Implemented
- [x] Top-N optimization (LIMIT + ORDER BY) - Delivered in Phase 8 (folded onto the scan / pushed as `ORDER BY … LIMIT`)

### 6.6 Other Optimizations (pending)
- [ ] Redundant join elimination (using foreign key constraints)
- [ ] Self-join elimination
- [ ] Common subexpression elimination (CSE)
- [ ] Filter merging and simplification (using Phase 4 expression rewriter)
- [ ] Constant predicate evaluation (eliminate always-true/false filters)

**Note**: These are advanced optimizations, deferred for now

### 6.7 Join Pushdown (from Phase 2, section 2.4) (done)
- [x] Detect when join can be pushed to single data source
  - [x] Both tables on same datasource
  - [x] No incompatible operations between tables
- [x] Implement remote join pushdown
  - [x] Generate SQL for entire join subplan
  - [x] Execute on remote datasource
  - [x] Replace local join with remote scan

**Note**: Delivered in Phase 8 via `optimizer/single_source_pushdown.py`
(`SingleSourcePushdown.try_build` → `PhysicalRemoteQuery`) — pushes the largest
same-source subtree (all join shapes incl. SEMI/ANTI/LEFT) as one remote query.

### 6.8 Aggregate Pushdown (from Phase 3, section 3.3) (partial-aggregation split deferred)
- [x] Detect when aggregation can be pushed to data source
  - [x] Single table aggregation → full pushdown
  - [x] Post-join aggregation → pushed when the whole join+aggregate is single-source
- [ ] Implement partial aggregation strategy → moved to **Phase 10** (cost-based)
  - [ ] Partial aggregate on each source (local aggregation)
  - [ ] Final aggregate locally (combine partial results)
  - [ ] Works for SUM, COUNT, MIN, MAX (not AVG directly)
- [ ] Handle DISTINCT aggregates → moved to **Phase 10**
  - [ ] COUNT(DISTINCT) requires special handling
  - [ ] May need to fetch distinct values then count locally

**Note**: Single-source aggregate pushdown delivered in Phase 8. Cross-source
*partial* aggregation (push partials to each source, combine locally) is a
cost-driven choice and is deferred to Phase 10; today a cross-source aggregate
runs in the merge engine after materializing its inputs.

### 6.9 Testing (done)
- [x] Test each optimization rule independently - 48 tests passing
- [x] **NEW**: Test optimization bug fixes - 7 tests (test_optimization_bugs.py)
- [x] **NEW**: Test additional optimization bugs - 20 tests (test_optimization_bugs_additional.py)
- [x] Test predicate pushdown with various filters - 7 tests
  - [x] Test push to scan - 1 test
  - [x] Test merge adjacent filters - 1 test
  - [x] Test push through projection - 1 test
  - [x] Test push below join (left side) - 1 test
  - [x] Test push below join (right side) - 1 test
  - [x] Test keep above join (both sides) - 1 test
  - [x] Test no pushdown needed - 1 test
- [x] Test projection pushdown and column pruning - 6 tests
  - [x] Test collect columns from scan with filter - 1 test
  - [x] Test prune unused columns from scan - 1 test
  - [x] Test keep columns needed by filter - 1 test
  - [x] Test collect columns from project - 1 test
  - [x] Test extract columns from binary op - 1 test
  - [x] Test extract columns from nested expr - 1 test
- [x] Test limit pushdown - 3 tests
- [x] Test combined optimizations (multiple rules) - 2 tests
- [x] Test optimizer with rule application engine - 3 tests
- [ ] Test join reordering with 3-5 table joins - Deferred
- [ ] Test join pushdown to same datasource - Deferred
- [ ] Test aggregate pushdown (single table and partial) - Deferred
- [ ] Verify plan correctness after optimization (results unchanged) - Deferred to integration tests
- [ ] Benchmark query performance improvement - Deferred
- [ ] Compare optimized vs unoptimized execution times - Deferred

**Deliverable**: **Substantially Complete** - Core optimization rules (predicate, projection, limit pushdown + join filter pushdown + column pruning) implemented with 48 comprehensive tests, all 10 critical bugs fixed

**Implementation Summary**:
- **PredicatePushdownRule** (federated_query/optimizer/rules.py:47-275):
  - Pushes filters to scan nodes
  - Merges adjacent filters
  - Pushes filters through projections
  - Pushes filters below joins (split by join side)
  - 229 lines, cyclomatic complexity ≤ 4 per function
  - Helper methods for column extraction and join side analysis
- **ProjectionPushdownRule** (federated_query/optimizer/rules.py:278-467):
  - Collects required columns from plan tree
  - Extracts columns from expressions (binary, unary, function calls)
  - **NEW**: Prunes unused columns from scan nodes
  - **NEW**: Propagates required columns through filters
  - 190 lines, cyclomatic complexity ≤ 4 per function
  - Helper methods: _prune_scan_columns, _get_required_for_subtree
- **LimitPushdownRule** (federated_query/optimizer/rules.py:470-498):
  - Pushes limits through projections and filters
  - Handles limit with offset
  - 49 lines, cyclomatic complexity ≤ 4 per function
- **Test Coverage**:
  - tests/test_logical_optimization.py: 21 tests across 5 test classes
  - tests/test_optimization_bugs.py: 7 critical bug fix tests
    - Limit pushdown semantics (2 tests)
    - Column pruning with SELECT * (2 tests)
    - Outer join filter pushdown safety (3 tests)
  - **NEW**: tests/test_optimization_bugs_additional.py: 20 additional bug fix tests
    - Filter through projection pushdown (2 tests)
    - Column detection for wrapped joins (2 tests)
    - FunctionCall column extraction (3 tests)
    - Table qualifier handling (4 tests)
    - Table alias handling (3 tests)
    - Column pruning preserving join keys (3 tests)
    - Aggregate column pruning preserving child requirements (3 tests)
  - Total: 48 tests, all passing

**Bug Fixes Implemented**:
1. **Limit Pushdown**: Fixed incorrect pushdown through filters that changed query semantics
2. **Column Pruning**: Fixed SELECT * losing columns when filters present
3. **Outer Join Safety**: Fixed unsafe filter pushdown below outer joins
4. **Filter Through Projection**: Fixed filter never actually pushing below projection
5. **Wrapped Join Column Detection**: Fixed empty column sets for Filter/Limit wrapped joins
6. **FunctionCall Column Extraction**: Fixed _extract_column_refs ignoring FunctionCall expressions, causing incorrect join filter pushdown
7. **Table Qualifier Handling**: Fixed predicate pushdown ignoring table qualifiers, causing filters on same-named columns (orders.id vs customers.id) to be misrouted to wrong side of join
8. **Table Alias Handling**: Fixed predicate pushdown failing for aliased tables (FROM users u WHERE u.age > 18) by adding alias field to Scan nodes and using alias when qualifying column names
9. **Column Pruning Join Keys**: Fixed _collect_required_columns stopping at Projection nodes without recursing into input, causing join keys and filter columns to be pruned from scans and breaking queries like SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id
10. **Aggregate Column Pruning**: Fixed _collect_required_columns stopping at Aggregate nodes without recursing into input, causing join keys and filter columns from aggregate children to be pruned (e.g., SELECT c.country, SUM(o.total) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.country would lose join keys)

**Future Work** (remaining Phase 6 tasks):
- Implement join reordering with cost model integration
- Implement join pushdown to same datasource (requires SQL generation)
- Implement aggregate pushdown (requires SQL generation)
- Add integration tests with end-to-end query execution
- Performance benchmarks comparing optimized vs unoptimized plans

---

## Phase 7: Decorrelation (Week 11) COMPLETED

**Status:** COMPLETE — pattern-based decorrelation in
`optimizer/decorrelation.py`. Recognized subquery shapes become flat joins;
unsupported shapes **fail fast** with `DecorrelationError` (never wrong answers).
The remaining general-fallback work is Phase 9.

**Goal**: Remove correlated subqueries

### 7.1 Subquery Support (done)
- [x] Add Subquery logical plan node
- [x] Parse scalar subqueries
- [x] Parse EXISTS/NOT EXISTS
- [x] Parse IN/NOT IN with subqueries
- [x] Parse ANY/ALL

### 7.2 Decorrelation Transforms (done)
- [x] Detect correlated columns
- [x] Transform EXISTS to SEMI JOIN
- [x] Transform NOT EXISTS to ANTI JOIN
- [x] Transform IN to SEMI JOIN (with exact three-valued NULL semantics for NOT IN)
- [x] Convert scalar subqueries to LEFT OUTER JOIN (keyed aggregate + cardinality guard)
- [x] Handle ANY/ALL with aggregations

### 7.3 Testing (done)
- [x] Test correlated subqueries before/after decorrelation (`tests/e2e_decorrelation/`)
- [x] Verify correctness of transformations
- [x] Test nested subqueries
- [x] Fail-fast tests for every unsupported shape (`test_error_cases.py`)

**Deliverable**: Efficient execution of queries with subqueries

---

## Phase 8: Pushdown Breadth, Decorrelation & Merge Engine COMPLETED

**Status:** COMPLETE — full suite **809 passing / 0 failed / 0 xfailed**.

> **Scope note:** the original §8.1–8.5 plan ("generate multiple physical plans
> and choose the best") was cost-based physical planning. That turned out to be a
> phase of its own and has moved to **Phase 10**. What actually shipped here is
> pushdown breadth + decorrelation coverage + CTEs + the merge engine, captured
> below. The core principle delivered is **"No Subqueries in the Physical Plan"**:
> decorrelation produces a flat join plan and single-source pushdown renders it as
> SQL joins — never a re-correlated `EXISTS`/`IN`/scalar `(SELECT …)`.

### 8.1 Single-source pushdown (largest same-source subtree → one remote query) (done)
The planner tries, top-down, to render the largest same-source subtree as one
remote SQL query via `optimizer/single_source_pushdown.py`
(`SingleSourcePushdown.try_build` → `PhysicalRemoteQuery`). What pushes:
- [x] Filters (`WHERE`) and projections (column pruning), incl. computed
      projections (`UPPER`, `||`, `*`, `CASE`, `CAST`).
- [x] Aggregates + `GROUP BY` + `HAVING`.
- [x] Joins of every shape — N-way, `FULL OUTER`, self-joins, non-equi / `OR` /
      computed conditions, and the `SEMI`/`ANTI`/`LEFT` joins decorrelation
      produces. No subquery expression is ever re-created in the pushed SQL.
- [x] Derived tables (`FROM (SELECT …) AS t`), same-source `LATERAL`, and set
      operations (`UNION`/`INTERSECT`/`EXCEPT`) as a subquery body.
- [x] CTEs (`WITH`, incl. `RECURSIVE`) — a same-source `WITH` pushed whole.
- [x] `ORDER BY` / `LIMIT` (incl. Top-N folded onto a scan).

### 8.2 Decorrelation coverage (pattern-based) (done)
- [x] `EXISTS`/`NOT EXISTS` → SEMI/ANTI; `IN`/`NOT IN` (incl. tuple IN) with exact
      three-valued NULL semantics; `ANY`/`SOME`/`ALL`.
- [x] Correlated & uncorrelated scalar subqueries → LEFT join to a keyed aggregate
      (+ COALESCE for COUNT, runtime cardinality guard).
- [x] Correlated `ORDER BY … LIMIT n` (pick-one / latest-per-key) → LEFT join +
      order-aware `GroupedLimit` (pushed to the merge engine as
      `ROW_NUMBER() OVER (PARTITION BY …)`).
- [x] Value/scalar bodies topped by `Filter` (HAVING) or `Sort` (ORDER BY/LIMIT)
      peel (uncorrelated). *(old gap A)*
- [x] Set-operation subquery body binds & decorrelates. *(old gap B)*
- [x] Non-equality correlation through an aggregate/limit → `LateralJoin`
      (dependent join); user-written `LATERAL` supported; cross-source LATERAL
      runs in the merge engine with domain reduction.

### 8.3 Merge engine for cross-source set work (done)
Each operator materializes inputs as Arrow once, registers them, and runs SQL in
an in-memory DuckDB (`executor/merge_engine.py`) — never hand-rolled Python loops.
- [x] `PhysicalHashJoin` / `PhysicalNestedLoopJoin` (all shapes) with cross-source
      **dynamic filtering / semi-join reduction** (build-side keys pushed into the
      probe as `IN`/range).
- [x] `PhysicalHashAggregate`, `PhysicalSort`, `PhysicalUnion`,
      `PhysicalSetOperation`, `PhysicalGroupedLimit` (per-key limit as a window).
- [x] `PhysicalLateralJoin` (cross-source dependent join, domain reduction) and
      `PhysicalCTE` / `PhysicalCTEScan` / `PhysicalCTEMergeQuery` (cross-source
      CTEs: materialize-once producer, or whole-`WITH` recursion in the engine).
- [x] Python paths remain only as a no-engine fallback or for genuinely
      un-renderable expressions (e.g. `SUM(a*b)` in a `GROUP BY` key) — sound
      degradations, not the default.

### 8.4 Additional SQL surface delivered here (done)
- [x] Date/time pushdown (G6): `EXTRACT`, `DATE_TRUNC`, `INTERVAL`, `AGE`,
      `CURRENT_DATE`.
- [x] Aggregate `FILTER (WHERE …)` and `NATURAL` / `USING` join pushdown (G7).

### 8.5 Testing (done)
- [x] `tests/e2e_pushdown/test_subqueries.py` asserts the join shape + a global
      no-subquery-expression invariant (`_assert_no_subquery_expressions`).
- [x] `tests/e2e_decorrelation/` green; per-operator merge-engine parity tests.
- [x] Cross-source LATERAL & CTE suites (`test_cross_source_lateral.py`,
      `test_cross_source_ctes.py`).

**Deliverable**: A subquery-free physical plan, uniform single-source pushdown
of joins/aggregates/CTEs/set-ops, and a merge engine for all cross-source work.

---

## Phase 9: General Dependent-Join Decorrelation + Cross-Source Correlated Fallback

**Status:** NOT STARTED — **next phase; lands before the cost work (Phase 10)**
because its cases and tests are the machinery cost-based planning builds on.

**Goal**: Move from pattern-based decorrelation (fast paths per recognized shape)
to a **general dependent join** (Neumann & Kemper, *"Unnesting Arbitrary Queries"*,
2015): emit a dependent join, then push it down with algebraic rules until the
correlation disappears. The pattern fast-paths stay as optimizations; the general
fallback removes every "unsupported shape" fail-fast and guarantees a
subquery-free physical plan unconditionally. **Physical subquery planning stays a
last resort** — ideally never reached.

### 9.1 General dependent join
- [ ] Implement the general dependent join + algebraic push-down rules.
- [ ] Keep the pattern fast-paths; route only un-flattenable shapes to the fallback.

### 9.2 Subsume the remaining fail-fast gaps
Each raises `DecorrelationError` today (never a wrong answer) and has a test in
`tests/e2e_decorrelation/test_error_cases.py`:
- [ ] Skip-level correlation (references a relation 2+ levels up) — `test_skip_level_correlation`
- [ ] Subquery in `GROUP BY` / aggregate-argument position — `test_subquery_in_group_by`
- [ ] `OFFSET` in a correlated subquery — `test_offset_in_correlated_subquery`
- [ ] Multi-column scalar / quantified subquery — `test_quantified_comparison_multi_column_subquery`
- [ ] `SELECT *` value subquery — `test_select_star_value_subquery`
- [ ] Subquery in a non-INNER join `ON`; multi-row `VALUES` subquery; two
      correlation equalities over a global (ungrouped) aggregate — guarded in
      `decorrelation.py`, no dedicated test yet

### 9.3 Cross-source correlated-subquery fallback (old "cluster D")
- [ ] A correlated subquery that can't decorrelate same-source falls back to the
      cross-source dependent-join path, reusing the LATERAL / CTE
      materialize-and-register + domain-reduction machinery (`PhysicalLateralJoin`,
      `PhysicalCTE*`).
- [ ] **Build the missing test scaffold first**: the `multi_source_env` fixture
      lacks the tables `test_cross_datasource_subquery_fallback` names — add the
      fixture + tests before the implementation.

**Deliverable**: No correlated subquery is ever left un-unnested; cross-source
correlated subqueries execute via the dependent-join fallback.

---

## Phase 10: Cost-Based Optimization

**Status:** NOT STARTED — the real "choose the best physical plan" work
(originally mis-scoped as Phase 8 §8.1–8.5). Bigger than first thought; depends on
the Phase 5 cost model and the Phase 9 machinery/tests.

**Goal**: Generate candidate physical plans and choose the minimum-cost one;
reorder joins and pick join strategies using the cost model.

### 10.1 Physical plan generation & selection
- [ ] Physical plan generator that enumerates candidates (not 1:1 logical→physical).
- [ ] Enumerate join strategies per join: hash (left/right build), nested-loop,
      broadcast.
- [ ] Enumerate scan strategies: remote scan, remote scan + filters, cached scan.
- [ ] Estimate cost per candidate (Phase 5 cost model) and choose the minimum;
      plan comparison + ranking.

### 10.2 Join reordering (needs the cost model)
- [ ] DP for small join graphs (<10 tables): build valid join trees, cost them,
      memoize. *(`JoinReorderingRule` currently `raise NotImplementedError()` and
      is not registered in any pipeline — wire it once the cost integration lands.)*
- [ ] Greedy heuristic for large graphs (≥10 tables) using Phase 5 cardinality.
- [ ] Preserve join semantics (outer-join order); handle cross joins.

### 10.3 Broadcast join
- [ ] Implement a broadcast operator (no `PhysicalBroadcastJoin` exists yet despite
      the CLAUDE.md reference).
- [ ] Detect when to broadcast (small build side); handle multiple broadcasts;
      memory management for broadcast data.

### 10.4 Cost-driven aggregate pushdown
- [ ] Partial aggregation split (partial on each source → combine locally) for
      SUM/COUNT/MIN/MAX; `COUNT(DISTINCT)` handling. *(carried from Phase 6.8.)*

### 10.5 Testing
- [ ] Test physical-plan generation & min-cost selection.
- [ ] Verify join-strategy selection and reordering on 3–5 table joins.
- [ ] Benchmark different strategies (note: engine is I/O-bound — validate that
      cost-based selection actually moves wall-clock before over-investing).

**Deliverable**: Best physical plan selected based on the cost model.

---

## Phase 11: Advanced Execution Features

**Status:** NOT STARTED
**Goal**: Improve execution performance with parallel execution and advanced memory management

**Prerequisites**:
- Basic execution engine from Phase 1 (working)
- Join operators from Phase 2 (working)
- Aggregation operators from Phase 3 (working)

### 11.1 Data Gathering (from Phase 2, section 2.3)
- [ ] Implement Gather operator (fetch remote data into local execution)
  - [ ] Fetch data from datasource as Arrow batches
  - [ ] Support streaming (don't materialize all at once)
  - [ ] Handle backpressure
- [ ] Handle parallel fetching from multiple sources
  - [ ] Thread pool or async I/O for concurrent fetches
  - [ ] Coordinate multiple Gather operators in one query
- [ ] Implement streaming for large results
  - [ ] Batched iteration (already partially done with Arrow batches)
  - [ ] Configurable batch size
  - [ ] Lazy evaluation where possible

### 11.2 Parallel Execution
- [ ] Parallel data fetching from multiple sources (expand 9.1)
- [ ] Parallel hash join build
  - [ ] Parallelize hash table construction
  - [ ] Parallel probe phase
- [ ] Thread pool for execution
  - [ ] Configurable thread pool size
  - [ ] Work-stealing scheduler
  - [ ] Avoid thread contention

### 11.3 Memory Management
- [ ] Track memory usage per operator
  - [ ] Monitor hash table sizes
  - [ ] Monitor buffer sizes
  - [ ] Global memory tracking
- [ ] Implement spill-to-disk for hash join (from Phase 2, section 2.3)
  - [ ] Partition hash table when memory limit exceeded
  - [ ] Spill partitions to temporary files
  - [ ] Load partitions back for probing
  - [ ] Clean up temporary files
- [ ] Implement spill-to-disk for hash aggregate
  - [ ] Similar partitioning strategy
  - [ ] External merge for final aggregation
- [ ] Configurable memory limits
  - [ ] Global memory limit
  - [ ] Per-operator memory limits
  - [ ] Graceful degradation when limits hit

### 11.4 Streaming and Pipelining
- [ ] Implement batched iteration (Arrow record batches) - Already partially done
- [ ] Pipeline compatible operators
  - [ ] Identify pipeline breakers (sort, hash join build, aggregate)
  - [ ] Maximize pipelining where possible
- [ ] Streaming aggregation where possible
  - [ ] Streaming for pre-sorted inputs
  - [ ] Hybrid hash/streaming aggregate

### 11.5 Testing
- [ ] Test Gather operator with various table sizes
- [ ] Test parallel fetching from multiple datasources
- [ ] Test parallel execution correctness (results match sequential)
- [ ] Test memory limits and spilling
  - [ ] Force spilling with small memory limits
  - [ ] Verify correctness after spilling
- [ ] Benchmark parallel vs sequential execution
- [ ] Benchmark with and without spilling
- [ ] Stress test with very large datasets

> Note: general dependent-join decorrelation + cross-source correlated-subquery
> fallback (the old §9.6) moved **up** to **Phase 9** — it precedes the cost work.

**Deliverable**: Production-ready parallel execution engine with robust memory management

---

## Phase 12: Window Functions + Remaining SQL Breadth (Week 15)

**Goal**: Close the remaining SQL surface. Most of this phase's original scope
(sorting, set ops, CTEs, date/time, FILTER, NATURAL/USING) shipped in Phase 8;
**user-facing window functions are the only real gap left.**

### 12.1 Sorting (delivered in Phase 8)
- [x] Add Sort logical/physical plan nodes
- [x] Implement sort operator (external sort handled by the merge engine / DuckDB)
- [x] Push ORDER BY to data sources
- [x] Combine with LIMIT for Top-N optimization

### 12.2 Set Operations (delivered in Phase 8)
- [x] Support UNION / UNION ALL
- [x] Support INTERSECT
- [x] Support EXCEPT
- [x] Implemented via the merge engine (`PhysicalUnion` / `PhysicalSetOperation`)

### 12.3 Window Functions (the remaining gap)
- [ ] Parse window functions (user-facing; `ROW_NUMBER()` is used internally for
      `GroupedLimit` but window functions are not yet a user feature —
      `test_windowed_subquery_not_supported` currently fails fast)
- [ ] Add Window logical/physical plan nodes
- [ ] Implement window function evaluation
- [ ] Push to data sources when possible

### 12.4 CTEs (Common Table Expressions) (delivered in Phase 8)
- [x] Parse WITH clauses
- [x] CTE evaluation strategies
  - [x] Inline / push same-source CTEs as one remote `WITH`
  - [x] Materialize-once producer for cross-source CTEs
- [x] Handle recursive CTEs (`WITH RECURSIVE`, incl. cross-source via the merge engine)

### 12.5 Testing
- [x] Test ORDER BY with various expressions
- [x] Test set operations
- [ ] Test window functions
- [x] Test CTEs (`test_ctes.py`, `test_cross_source_ctes.py`)

**Deliverable**: Window-function support; all other advanced SQL surface already
shipped in Phase 8.

---

## Phase 13: Production Readiness (Week 16)

**Goal**: Make the engine production-ready

### 13.1 Error Handling
- [ ] Comprehensive error messages
- [ ] Graceful degradation
- [ ] Connection retry logic
- [ ] Transaction rollback on errors

#### 13.1.1 Structured error codes (`error_codes.md`)
**Goal**: Every engine error carries a stable, numbered code so a failure can be
pinpointed to its origin instead of surfacing as "error: <free text>". Mirrors
how real databases do it — MySQL numeric codes (e.g. `ER_DUP_FIELDNAME 1060`) +
SQLSTATE, PostgreSQL 5-char SQLSTATE classes, DuckDB typed exceptions
(`BinderException`, `ConversionException`, ...). The CLI should print something
like `error FEDQ-1402: duplicate output column 'id' in pushed join`.

- [ ] Define a code scheme. Proposal: `FEDQ-<NNNN>` where the leading digit(s)
      bucket by subsystem so the number alone tells you where it came from:
  - `1xxx` parser / dialect
  - `2xxx` binder / catalog / type mapping
  - `3xxx` decorrelation
  - `4xxx` logical optimizer / planner / pushdown
  - `5xxx` physical execution (operators, joins, aggregates)
  - `6xxx` datasource / remote query (connection, result decoding)
  - `9xxx` internal invariant / "should not happen"
- [ ] Introduce a base `FedQError(code, message, *, context=None)` (and a small
      hierarchy: `ParseError`, `BindingError`, `DecorrelationError`,
      `PlanningError`, `ExecutionError`, `DataSourceError`) so each raise site
      passes a code. Keep the fail-fast rule (CLAUDE.md) — codes annotate, they
      do not add swallowing/wrapping.
- [ ] Replace raw `raise ValueError(...)` / `RuntimeError(...)` / ad-hoc
      messages on the user-facing paths with coded errors. The CLI formats them
      as `error FEDQ-NNNN: <message>` (and shows context/origin when present).
- [ ] Maintain `error_codes.md` at the repo root: a table of every allocated
      code with its meaning, subsystem, and a one-line cause/fix note. New
      errors get the next free code in their bucket; codes are never reused.
- [ ] Add a test that asserts no two raise sites share a code and that every
      code in the catalog is referenced (and vice versa).

NOTE (2026-06-12): the duplicate-column crash ("Arrays were not all the same
length: 1 vs 2", PostgreSQL connector collapsing same-named result columns) is
already fixed, so it gets no code retroactively — it is the motivating example
for why this scheme is needed. When implemented, that class of datasource
result-decoding error would live in the `6xxx` bucket.

### 13.2 Logging and Observability
- [ ] Structured logging
- [ ] Query plan logging (EXPLAIN)
- [ ] Execution statistics
- [ ] Performance metrics (latency, throughput)

### 13.3 Configuration and Tuning
- [ ] Expose tuning parameters
  - [ ] Memory limits
  - [ ] Parallelism degree
  - [ ] Cost model parameters
- [ ] Configuration validation
- [ ] Environment variable support

### 13.4 Documentation
- [ ] API documentation
- [ ] Configuration guide
- [ ] Query optimization guide
- [ ] Examples and tutorials

### 13.5 Performance Testing
- [ ] Create benchmark suite
- [ ] TPC-H style queries
- [ ] Measure and optimize hot paths
- [ ] Profiling and optimization

### 13.6 Testing
- [ ] Integration tests for all features
- [ ] Edge case tests
- [ ] Stress tests
- [ ] Fuzzing tests

**Deliverable**: Production-ready federated query engine

---

## Phase 14: Advanced Features (Future)

**Goal**: Advanced optimizations and features

### 14.1 Adaptive Query Execution
- [ ] Collect runtime statistics
- [ ] Re-optimize during execution
- [ ] Switch join strategies dynamically

### 14.2 Query Result Caching
- [ ] Cache small query results
- [ ] Cache invalidation strategy
- [ ] Materialized views

### 14.3 More Data Sources
- [ ] MySQL connector
- [ ] SQLite connector
- [ ] REST API connector
- [ ] CSV/Parquet file connector

### 14.4 Write Operations
- [ ] INSERT support
- [ ] UPDATE support
- [ ] DELETE support
- [ ] Distributed transactions (2PC)

### 14.5 Advanced Optimizations
- [x] Semi-join pushdown (reduce data transfer) — delivered in Phase 8 (build-side
      keys pushed into the probe as `IN`/range)
- [ ] Bloom filter pushdown
- [x] Dynamic filter pushdown — delivered in Phase 8 (cross-source dynamic filtering)
- [ ] Query compilation (LLVM)

---

## Milestones

1. **Milestone 1** COMPLETED (Phase 1): Single-table queries work
2. **Milestone 2** COMPLETED (Phase 2): Joins across data sources work
3. **Milestone 3** COMPLETED (Phase 3): Aggregations work
4. **Milestone 4** COMPLETED (Phase 4): Expression optimization work
5. **Milestone 5** COMPLETED (Phase 5): Cost model and statistics collection
6. **Milestone 6** COMPLETED (Phase 6): Logical optimization pipeline (predicate/projection/limit pushdown, column pruning)
7. **Milestone 7** COMPLETED (Phase 7–8): Decorrelation + uniform single-source pushdown + merge engine + CTEs ("No Subqueries in the Physical Plan")
8. **Milestone 8** PLANNED (Phase 9): General dependent-join decorrelation + cross-source correlated-subquery fallback
9. **Milestone 9** PLANNED (Phase 10): Cost-based physical planning (plan selection, join reordering, broadcast)
10. **Milestone 10** PLANNED (Phase 11): Advanced execution with parallelism + memory management
11. **Milestone 11** PLANNED (Phase 12–13): Window functions + production readiness

---

## Development Principles

1. **Test-Driven**: Write tests before/during implementation
2. **Incremental**: Each phase should produce working software
3. **Measurable**: Benchmark performance at each phase
4. **Correctness First**: Get it right, then make it fast
5. **Simplicity**: Start simple, add complexity only when needed

---

## Success Metrics

- **Correctness**: 100% of test queries return correct results
- **Performance**:
  - Single-table queries: <100ms latency
  - Join queries: <500ms for typical workloads
  - Aggregation queries: <1s for typical workloads
- **Optimization**:
  - 90%+ of filters pushed down
  - Optimal join order for <10 table joins
  - Correct join strategy selection 95%+ of time
- **Scalability**:
  - Handle queries with 10+ tables
  - Handle result sets with 1M+ rows
  - Support 100+ concurrent queries

---

This task breakdown provides a clear roadmap from basic functionality to a production-ready federated query engine. Each phase builds on the previous one, with clear deliverables and testing requirements.

---

# Appendix: Architecture Quick Map & Current Capabilities

*(Merged 2026-06-25 from the former `TODO-next.md`, `pushdown-status.md`, and
`decorrelation-gaps.md` handoff docs — this is now the single source of truth.)*

## Pipeline

`processor/query_executor.py::_plan_pipeline`:
**preprocess → parse → bind → decorrelate → optimize → physical plan.**

- **Decorrelation** — `optimizer/decorrelation.py` (pattern-based; emits
  SEMI/ANTI/LEFT/LATERAL joins; fail-fast `DecorrelationError` on unsupported
  shapes). The general dependent-join fallback is Phase 9.
- **Single-source pushdown** — `optimizer/single_source_pushdown.py`, invoked by
  `optimizer/physical_planner.py::_plan_node` via `try_build` (top-down). Renders
  the largest same-source subtree as one `PhysicalRemoteQuery`.
- **Merge engine** — `executor/merge_engine.py` (in-memory DuckDB). Each
  cross-source operator materializes inputs as Arrow once, registers them, runs
  SQL. `MergeEngine.schema(sql, inputs)` reads the Arrow reader schema (do **not**
  use a `LIMIT 0` probe — it yields zero batches and an empty schema).
- Remote SQL is built as a string then re-parsed via `datasource.parse_query` for
  the EXPLAIN document; on execution it is re-rendered to the source dialect.

## Pushdown capability matrix

**Pushes to a single source (one remote query):** filters; projections incl.
computed; aggregates + GROUP BY + HAVING; joins of every shape incl.
SEMI/ANTI/LEFT from decorrelation (no subquery expression re-created); derived
tables; same-source LATERAL; set operations as a subquery body; CTEs incl.
RECURSIVE; ORDER BY / LIMIT incl. Top-N folded onto a scan; date/time functions;
aggregate FILTER; NATURAL/USING joins.

**Runs locally in the merge engine (cross-source):** hash / nested-loop joins
(all shapes) with dynamic filtering / semi-join reduction; hash aggregate; sort;
union; set operation; per-key grouped limit (window); cross-source LATERAL
(dependent join with domain reduction); cross-source CTEs (materialize-once
producer / whole-`WITH` recursion). Python row-loops remain only as a no-engine
fallback or for genuinely un-renderable expressions.

**Not yet pushed / out of scope:** see Phase 9 (dependent-join gaps + cross-source
correlated fallback), Phase 10 (cost-based plan selection, join reordering,
partial aggregation), Phase 12 (window functions).

## Decorrelation north star & remaining gaps

**North star:** the logical phase should fully unnest every subquery so the
physical planner never sees a subquery expression — a flat relational plan
(scans, joins incl. semi/anti, aggregates, set ops), exactly the shape DuckDB
feeds its executor. Reference: Neumann & Kemper, *"Unnesting Arbitrary Queries"*
(2015). Physical subquery planning is a last resort, ideally never reached.

**Remaining fail-fast gaps** (all raise `DecorrelationError`, never wrong
answers; tests in `tests/e2e_decorrelation/test_error_cases.py`) — closed by
Phase 9:

| Gap | Test |
|-----|------|
| Skip-level correlation (relation 2+ levels up) | `test_skip_level_correlation` |
| Subquery in `GROUP BY` / aggregate-argument position | `test_subquery_in_group_by` |
| `OFFSET` in a correlated subquery | `test_offset_in_correlated_subquery` |
| Multi-column scalar / quantified subquery | `test_quantified_comparison_multi_column_subquery` |
| `SELECT *` value subquery | `test_select_star_value_subquery` |
| Non-INNER join `ON` subquery; multi-row `VALUES`; two equalities over a global aggregate | guarded in `decorrelation.py` (no dedicated test yet) |

## Running tests / coverage

- Full suite: `make pg-start` then
  `POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python -m pytest tests/ -q`
  (809 passing as of 2026-06-25).
- Coverage: plain `pytest-cov` dies on `import duckdb` under instrumentation
  (numpy 2.4 + coverage C-ext reimport crash). Use the pre-import shim
  `/tmp/covrun.py` (imports duckdb/numpy/pyarrow before `coverage.start()`) with
  the `/workspace/venv-cov` venv (numpy 2.3.5).

## Coding rules (CLAUDE.md / AGENTS.md) — enforce

No list comprehensions; no bare except; ≤20 lines & cyclomatic ≤4 per function;
comment every function; no "pointless" verbose code; `black` clean; fail fast,
don't wrap exceptions; no compat cruft (delete, don't shim).
