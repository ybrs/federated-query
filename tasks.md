# Federated Query Engine - Implementation Tasks

## Overview

This document breaks down the implementation into phases. Each phase builds on the previous one, aiming for a working end-to-end system early, then adding optimizations and advanced features.

## Phase 0: Foundation ✅ COMPLETED

**Goal**: Set up project structure and basic infrastructure

- [x] Create project structure and directories
- [x] Set up dependencies (sqlglot, psycopg2, duckdb, pyarrow)
- [x] Create configuration system (YAML-based)
- [x] Implement data source catalog
- [x] Create basic logging and error handling
- [x] Set up testing framework (pytest)
- [x] Create development environment (Docker compose with PostgreSQL)

**Deliverable**: ✅ Project skeleton with configuration loading

**Notes**:
- All 50 tests passing
- PostgreSQL and DuckDB connectors fully implemented with connection pooling
- Catalog supports metadata discovery and table resolution
- Structured logging with JSON formatter available
- Skeleton classes created for parser, binder, executor, and plan nodes

---

## Phase 1: Basic Query Execution ✅ COMPLETED

**Goal**: Execute simple single-table queries on remote sources

**Status**: Fully implemented and tested

**Implementation Summary**:

All Phase 1 components have been successfully implemented with comprehensive test coverage.
The system can now execute queries like: `SELECT col1, col2 FROM datasource.table WHERE col1 > 10 LIMIT 100`

### 1.1 Parser and AST Conversion ✅
- [x] Integrate sqlglot parser (can parse SQL to AST)
- [x] Create logical plan node base classes (Scan, Project, Filter, Limit)
- [x] Implement AST to logical plan converter (parser.py:ast_to_logical_plan)
  - [x] Handle SELECT statements
  - [x] Handle FROM clauses (create Scan nodes)
  - [x] Handle WHERE clauses (create Filter nodes)
  - [x] Handle column selections (create Project nodes)
  - [x] Handle LIMIT clauses
- [x] Test parser with various simple queries (4 tests passing)

### 1.2 Catalog and Binder ✅
- [x] Implement catalog interface (Catalog class works)
- [x] Create schema metadata classes (Table, Column, Type)
- [x] Implement binder to resolve table/column references (binder.py:bind)
  - [x] Resolve table references using catalog
  - [x] Resolve column references
  - [x] Validate column existence
  - [x] Add data type information to expressions
- [x] Support multi-schema (datasource.schema.table)
- [x] Test binding with various table/column references (8 tests passing)

### 1.3 Data Source Connectors ✅
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

### 1.4 Basic Executor ✅
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
- [x] Implement physical project operator (PhysicalProject.execute())
  - [x] Project/select columns from input batches
  - [x] Handle column references
- [x] Implement physical limit operator (PhysicalLimit.execute())
  - [x] Limit output rows
  - [x] Handle offset correctly
- [x] Create simple physical planner (logical to physical, 1:1 mapping)
- [x] Executor infrastructure (calls plan.execute())

### 1.5 Testing ✅
- [x] Test single-table SELECT queries (7 end-to-end tests passing)
- [x] Test WHERE clause execution
- [x] Test column projection
- [x] Test LIMIT execution
- [x] Test end-to-end: SQL -> logical plan -> physical plan -> execution -> results

**Deliverable**: ✅ Fully functional - can execute `SELECT col1, col2 FROM datasource.table WHERE col1 > 10 LIMIT 100`

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

**Status:** ✅ COMPLETED
**Goal**: Execute joins across data sources
**Tests**: 5 join tests passing, 70 total tests

### 2.1 Logical Plan - Joins
- [x] Add Join logical plan node
- [x] Support join types (INNER, LEFT, RIGHT, FULL, CROSS)
- [x] Parse ON conditions and extract join predicates
- [x] Handle multi-way joins

### 2.2 Physical Join Operators
- [x] Implement Hash Join
  - [x] Build hash table from right side
  - [x] Probe with left side
  - [x] Handle NULL values correctly (basic INNER JOIN)
- [x] Implement Nested Loop Join (fallback for non-equi joins)
- [x] Implement cross-product handling
- [x] Schema resolution for joins

### 2.3 Data Gathering
- [ ] Implement Gather operator (fetch remote data) - DEFERRED
- [ ] Handle parallel fetching from multiple sources - DEFERRED
- [ ] Implement streaming for large results - DEFERRED
- [ ] Add memory management (spill to disk if needed) - DEFERRED

### 2.4 Join Strategy Selection (Basic)
- [x] Choose join operator based on join type (Hash vs Nested Loop)
- [x] Extract equi-join keys from conditions
- [x] Physical planning for joins
- [ ] Detect when join can be pushed to single data source - TODO Phase 3
- [ ] Implement remote join pushdown - TODO Phase 3

### 2.5 Testing
- [x] Test joins on same data source
- [x] Test JOIN with WHERE clauses
- [x] Test specific column selection in joins
- [x] Test SELECT * with joins
- [x] Verify logical plan creation for joins

**Deliverable**: ✅ Can execute `SELECT c.name, o.order_id, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > 100`

**Implementation Notes**:
- Parser extends sqlglot AST to logical Join nodes
- Binder handles multi-table column resolution with table aliases
- PhysicalHashJoin implements in-memory hash join algorithm
- PhysicalNestedLoopJoin serves as fallback for non-equi joins
- Physical planner chooses join strategy based on condition analysis
- All joins currently execute locally (cross-datasource gathering deferred)

---

## Phase 3: Aggregations and Grouping (Week 6)

**Goal**: Support GROUP BY and aggregation functions

### 3.1 Logical Plan - Aggregations
- [ ] Add Aggregate logical plan node
- [ ] Parse GROUP BY clauses
- [ ] Parse aggregate functions (SUM, COUNT, AVG, MIN, MAX)
- [ ] Handle HAVING clauses

### 3.2 Physical Aggregate Operators
- [ ] Implement Hash Aggregate
- [ ] Support grouping by multiple columns
- [ ] Implement aggregation functions
- [ ] Handle NULL values in aggregations

### 3.3 Aggregate Pushdown
- [ ] Detect when aggregation can be pushed to data source
- [ ] Implement partial aggregation strategy
  - [ ] Partial aggregate on each source
  - [ ] Final aggregate locally
- [ ] Handle DISTINCT aggregates

### 3.4 Testing
- [ ] Test simple aggregations (COUNT, SUM, AVG)
- [ ] Test GROUP BY with multiple columns
- [ ] Test HAVING clauses
- [ ] Test aggregation pushdown
- [ ] Test partial aggregation across sources

**Deliverable**: Execute `SELECT region, COUNT(*), AVG(amount) FROM orders GROUP BY region HAVING COUNT(*) > 100`

---

## Phase 4: Pre-Optimization and Expression Handling (Week 7)

**Goal**: Simplify expressions and perform basic optimizations

### 4.1 Expression System
- [ ] Create expression node classes
  - [ ] Literals, Column references
  - [ ] Binary/Unary operators
  - [ ] Function calls
  - [ ] CASE expressions
- [ ] Implement expression evaluator
- [ ] Type inference for expressions

### 4.2 Pre-Optimization Rules
- [ ] Constant folding (`1 + 2` → `3`)
- [ ] Expression simplification
  - [ ] `x AND TRUE` → `x`
  - [ ] `x OR FALSE` → `x`
  - [ ] `NOT (NOT x)` → `x`
- [ ] Predicate normalization (CNF conversion)
- [ ] Null handling simplification

### 4.3 Testing
- [ ] Test constant folding
- [ ] Test expression simplification
- [ ] Test complex WHERE clauses

**Deliverable**: Queries with complex expressions are simplified before execution

---

## Phase 5: Statistics and Cost Model (Week 8)

**Goal**: Implement statistics collection and cost estimation

### 5.1 Statistics Collection
- [ ] Define statistics schema (TableStats, ColumnStats)
- [ ] Implement statistics collector
  - [ ] Row counts
  - [ ] Column cardinality (NDV)
  - [ ] Null fractions
  - [ ] Data size estimation
- [ ] Statistics caching with TTL
- [ ] Support for sampling large tables

### 5.2 Cost Model
- [ ] Define cost model parameters (CPU, IO, Network costs)
- [ ] Implement cost estimation for each operator
  - [ ] Scan cost
  - [ ] Filter cost (with selectivity)
  - [ ] Join cost (nested loop, hash join)
  - [ ] Aggregate cost
- [ ] Implement cardinality estimation
  - [ ] Filter selectivity estimation
  - [ ] Join selectivity estimation
  - [ ] Independence assumption

### 5.3 Selectivity Estimation
- [ ] Implement selectivity for comparison operators
  - [ ] Equality: `1 / num_distinct`
  - [ ] Inequality: heuristic-based
- [ ] Implement selectivity for AND/OR
- [ ] Use histograms if available

### 5.4 Testing
- [ ] Test statistics collection
- [ ] Verify cost estimation for simple queries
- [ ] Compare estimated vs actual cardinalities

**Deliverable**: Cost estimates for all query plans

---

## Phase 6: Logical Optimization (Week 9-10)

**Goal**: Implement rule-based logical optimizations

### 6.1 Optimization Framework
- [ ] Create optimization rule interface
- [ ] Implement rule application engine
- [ ] Support multiple optimization passes
- [ ] Add rule ordering and fixed-point iteration

### 6.2 Predicate Pushdown
- [ ] Push filters through projections
- [ ] Push filters below joins
  - [ ] Handle join conditions correctly
  - [ ] Preserve semantics for outer joins
- [ ] Push filters to data sources (remote pushdown)
- [ ] Split complex predicates (conjunctions)

### 6.3 Projection Pushdown
- [ ] Column pruning (remove unused columns)
- [ ] Push projections through filters
- [ ] Push projections to data sources

### 6.4 Join Reordering
- [ ] Implement dynamic programming for small join graphs (< 10 tables)
- [ ] Implement greedy heuristic for large join graphs
- [ ] Consider selectivity and cardinality
- [ ] Preserve join semantics (left/right outer joins)

### 6.5 Other Optimizations
- [ ] Limit pushdown through projections and filters
- [ ] Redundant join elimination
- [ ] Common subexpression elimination
- [ ] Filter merging and simplification

### 6.6 Testing
- [ ] Test each optimization rule independently
- [ ] Test combined optimizations
- [ ] Verify plan correctness after optimization
- [ ] Benchmark query performance improvement

**Deliverable**: Optimized query plans with significant performance improvements

---

## Phase 7: Decorrelation (Week 11)

**Goal**: Remove correlated subqueries

### 7.1 Subquery Support
- [ ] Add Subquery logical plan node
- [ ] Parse scalar subqueries
- [ ] Parse EXISTS/NOT EXISTS
- [ ] Parse IN/NOT IN with subqueries
- [ ] Parse ANY/ALL

### 7.2 Decorrelation Transforms
- [ ] Detect correlated columns
- [ ] Transform EXISTS to SEMI JOIN
- [ ] Transform NOT EXISTS to ANTI JOIN
- [ ] Transform IN to SEMI JOIN
- [ ] Convert scalar subqueries to LEFT OUTER JOIN
- [ ] Handle ANY/ALL with aggregations

### 7.3 Testing
- [ ] Test correlated subqueries before/after decorrelation
- [ ] Verify correctness of transformations
- [ ] Test nested subqueries
- [ ] Performance comparison

**Deliverable**: Efficient execution of queries with subqueries

---

## Phase 8: Physical Planning and Join Strategies (Week 12-13)

**Goal**: Generate multiple physical plans and choose the best

### 8.1 Physical Plan Generation
- [ ] Create physical plan generator
- [ ] Enumerate join strategies for each join
  - [ ] Hash join (left/right build)
  - [ ] Nested loop join
  - [ ] Broadcast join
- [ ] Enumerate scan strategies
  - [ ] Remote scan
  - [ ] Remote scan with filters
  - [ ] Cached scan

### 8.2 Broadcast Join
- [ ] Implement broadcast operator
- [ ] Detect when to use broadcast (small table)
- [ ] Handle multiple broadcasts efficiently
- [ ] Memory management for broadcasted data

### 8.3 Remote Pushdown Planning
- [ ] Detect subplans that can execute on single source
- [ ] Generate SQL for pushed-down subplans
- [ ] Handle data source capabilities
- [ ] Fallback when pushdown not possible

### 8.4 Plan Selection
- [ ] Estimate cost for each physical plan
- [ ] Choose minimum cost plan
- [ ] Implement plan comparison and ranking

### 8.5 Testing
- [ ] Test physical plan generation
- [ ] Verify join strategy selection
- [ ] Test broadcast join with small tables
- [ ] Benchmark different strategies

**Deliverable**: Best physical plan selected based on cost model

---

## Phase 9: Advanced Execution Features (Week 14)

**Goal**: Improve execution performance

### 9.1 Parallel Execution
- [ ] Parallel data fetching from multiple sources
- [ ] Parallel hash join build
- [ ] Thread pool for execution

### 9.2 Streaming and Pipelining
- [ ] Implement batched iteration (Arrow record batches)
- [ ] Pipeline compatible operators
- [ ] Streaming aggregation where possible

### 9.3 Memory Management
- [ ] Track memory usage
- [ ] Implement spill-to-disk for hash join
- [ ] Implement spill-to-disk for hash aggregate
- [ ] Configurable memory limits

### 9.4 Testing
- [ ] Test parallel execution correctness
- [ ] Test memory limits and spilling
- [ ] Benchmark parallel vs sequential

**Deliverable**: Efficient parallel execution with memory management

---

## Phase 10: Additional SQL Features (Week 15)

**Goal**: Support more SQL features

### 10.1 Sorting
- [ ] Add Sort logical/physical plan nodes
- [ ] Implement sort operator (external sort if needed)
- [ ] Push ORDER BY to data sources
- [ ] Combine with LIMIT for Top-N optimization

### 10.2 Set Operations
- [ ] Support UNION / UNION ALL
- [ ] Support INTERSECT
- [ ] Support EXCEPT
- [ ] Implement as hash-based operators

### 10.3 Window Functions
- [ ] Parse window functions
- [ ] Add Window logical/physical plan nodes
- [ ] Implement window function evaluation
- [ ] Push to data sources when possible

### 10.4 CTEs (Common Table Expressions)
- [ ] Parse WITH clauses
- [ ] Implement CTE evaluation strategies
  - [ ] Inline small CTEs
  - [ ] Materialize large CTEs
- [ ] Handle recursive CTEs (future)

### 10.5 Testing
- [ ] Test ORDER BY with various expressions
- [ ] Test set operations
- [ ] Test window functions
- [ ] Test CTEs

**Deliverable**: Support for advanced SQL features

---

## Phase 11: Production Readiness (Week 16)

**Goal**: Make the engine production-ready

### 11.1 Error Handling
- [ ] Comprehensive error messages
- [ ] Graceful degradation
- [ ] Connection retry logic
- [ ] Transaction rollback on errors

### 11.2 Logging and Observability
- [ ] Structured logging
- [ ] Query plan logging (EXPLAIN)
- [ ] Execution statistics
- [ ] Performance metrics (latency, throughput)

### 11.3 Configuration and Tuning
- [ ] Expose tuning parameters
  - [ ] Memory limits
  - [ ] Parallelism degree
  - [ ] Cost model parameters
- [ ] Configuration validation
- [ ] Environment variable support

### 11.4 Documentation
- [ ] API documentation
- [ ] Configuration guide
- [ ] Query optimization guide
- [ ] Examples and tutorials

### 11.5 Performance Testing
- [ ] Create benchmark suite
- [ ] TPC-H style queries
- [ ] Measure and optimize hot paths
- [ ] Profiling and optimization

### 11.6 Testing
- [ ] Integration tests for all features
- [ ] Edge case tests
- [ ] Stress tests
- [ ] Fuzzing tests

**Deliverable**: Production-ready federated query engine

---

## Phase 12: Advanced Features (Future)

**Goal**: Advanced optimizations and features

### 12.1 Adaptive Query Execution
- [ ] Collect runtime statistics
- [ ] Re-optimize during execution
- [ ] Switch join strategies dynamically

### 12.2 Query Result Caching
- [ ] Cache small query results
- [ ] Cache invalidation strategy
- [ ] Materialized views

### 12.3 More Data Sources
- [ ] MySQL connector
- [ ] SQLite connector
- [ ] REST API connector
- [ ] CSV/Parquet file connector

### 12.4 Write Operations
- [ ] INSERT support
- [ ] UPDATE support
- [ ] DELETE support
- [ ] Distributed transactions (2PC)

### 12.5 Advanced Optimizations
- [ ] Semi-join pushdown (reduce data transfer)
- [ ] Bloom filter pushdown
- [ ] Dynamic filter pushdown
- [ ] Query compilation (LLVM)

---

## Milestones

1. **Milestone 1** (Week 3): Single-table queries work
2. **Milestone 2** (Week 5): Joins across data sources work
3. **Milestone 3** (Week 6): Aggregations work
4. **Milestone 4** (Week 10): Full optimization pipeline
5. **Milestone 5** (Week 13): Cost-based physical planning
6. **Milestone 6** (Week 16): Production-ready

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
