# Federated Query Engine - Architecture and Approach

## Overview

This is a production-grade federated query engine that executes SQL queries across multiple heterogeneous data sources (PostgreSQL, DuckDB) in an optimal way. The engine parses SQL, optimizes execution plans, and coordinates data movement to minimize latency and network transfer.

## Implementation Status

**Current Implementation State**: Phase 2 Complete, Phase 3 Starting

- ✅ **Phase 0**: Foundation - Project structure, configuration, catalog, data sources
- ✅ **Phase 1**: Basic Query Execution - Single-table SELECT queries with filters and limits
- ✅ **Phase 2**: Joins - Multi-table queries with hash joins and nested loop joins
- 🚧 **Phase 3**: Aggregations - GROUP BY and aggregate functions (IN PROGRESS)
- ⏳ **Future Phases**: Optimization, cost-based planning, decorrelation, advanced features

**Test Status**: 70 tests passing

## Goals

1. **Performance**: Execute federated queries with minimal data movement
2. **Correctness**: Produce correct results for complex queries including joins, aggregations, and subqueries
3. **Extensibility**: Easy to add new data sources and optimization rules
4. **Production-ready**: Proper error handling, logging, and observability

## Architecture Components

### 1. Configuration Layer

**Purpose**: Define available data sources and their capabilities

```yaml
datasources:
  postgres_prod:
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: analytics
    schemas: [public, staging]
    capabilities:
      - aggregations
      - joins
      - window_functions

  local_duckdb:
    type: duckdb
    path: /data/local.duckdb
    capabilities:
      - aggregations
      - joins
      - window_functions
```

**Key Features**:
- Declare data source capabilities for pushdown optimization
- Connection pooling configuration
- Statistics and metadata caching

### 2. Parser and Binder

**Purpose**: Parse SQL into AST and bind to schema metadata

**Components**:
- **Parser**: Uses sqlglot to parse SQL into AST
- **Catalog**: Maintains metadata about tables, columns, types across all data sources
- **Binder**: Resolves table/column references and validates types
- **Logical Plan Builder**: Converts bound AST to logical plan tree

**Logical Plan Nodes**:
- `Scan`: Read from a table (with optional filters)
- `Projection`: Column projection and expression evaluation
- `Filter`: Row filtering with predicates
- `Join`: Join operations (inner, left, right, full, cross)
- `Aggregate`: Grouping and aggregation
- `Sort`: Ordering
- `Limit`: Result limiting
- `Union/Intersect/Except`: Set operations
- `Subquery`: Correlated and uncorrelated subqueries

### 3. Pre-Optimization

**Purpose**: Basic algebraic simplifications before main optimization

**Rules**:
- Constant folding: `1 + 2` → `3`
- Expression simplification: `x AND TRUE` → `x`
- Predicate normalization: Convert to CNF/DNF
- Type coercion resolution
- Null handling simplification

### 4. Decorrelation

**Purpose**: Remove correlated subqueries for better optimization opportunities

**Approach**:
- Detect correlated columns in subqueries
- Transform EXISTS/NOT EXISTS to semi/anti joins
- Convert scalar subqueries to left outer joins
- Handle ANY/ALL using aggregations

**Example**:
```sql
-- Before decorrelation
SELECT * FROM orders o
WHERE EXISTS (
  SELECT 1 FROM customers c
  WHERE c.id = o.customer_id AND c.region = 'US'
)

-- After decorrelation
SELECT o.* FROM orders o
SEMI JOIN customers c ON c.id = o.customer_id
WHERE c.region = 'US'
```

### 5. Logical Optimization

**Purpose**: Transform logical plan for better performance

**Optimization Rules** (applied in multiple passes):

1. **Predicate Pushdown**:
   - Push filters below joins to reduce data volume
   - Push filters to data sources (remote filter pushdown)
   - Handle complex predicates with conjunctions

2. **Projection Pushdown**:
   - Eliminate unused columns early
   - Push projections to data sources

3. **Join Reordering**:
   - Use dynamic programming for small join graphs
   - Use greedy heuristics for large join graphs
   - Consider selectivity and cardinality

4. **Aggregate Pushdown**:
   - Push aggregations to data sources when possible
   - Partial aggregation for distributed execution

5. **Limit Pushdown**:
   - Push limits through projections and filters
   - Cannot push through certain joins/aggregations

6. **Join Elimination**:
   - Remove unnecessary joins (foreign key constraints)
   - Detect and eliminate self-joins

7. **Column Pruning**:
   - Remove unused columns from scans
   - Prune intermediate projections

### 6. Cost-Based Planning

**Purpose**: Estimate cost of different plan alternatives and choose the best

**Statistics Required**:
- Table row counts
- Column cardinality (number of distinct values)
- Column null ratio
- Data distribution (histograms for key columns)
- Data size (bytes)

**Cost Model**:
```
Cost = CPU_cost + IO_cost + Network_cost

CPU_cost = rows_processed × CPU_TUPLE_COST
IO_cost = pages_read × IO_PAGE_COST
Network_cost = bytes_transferred × NETWORK_BYTE_COST + RTT × NUM_ROUND_TRIPS
```

**Cost Estimation for Operations**:
- **Scan**: rows × (CPU + IO costs)
- **Filter**: input_rows × selectivity × CPU_cost
- **Join**: depends on join method (nested loop, hash, merge)
- **Aggregate**: input_rows × CPU_cost + hash_table_cost
- **Sort**: input_rows × log(input_rows) × CPU_cost

**Cardinality Estimation**:
- Use histograms and statistics when available
- Default selectivity factors for operators (<, =, LIKE, etc.)
- Independence assumption for multiple predicates
- Handle correlation hints from user

### 7. Physical Planning

**Purpose**: Convert logical plan to physical execution plan with concrete algorithms

**Physical Operators**:

1. **Scan Operators**:
   - `RemoteScan`: Execute scan on remote data source
   - `LocalScan`: Scan local data (cached or intermediate)
   - `IndexScan`: Use indexes when available

2. **Join Strategies**:
   - `HashJoin`: Build hash table from smaller side, probe with larger
   - `MergeJoin`: For sorted inputs
   - `NestedLoopJoin`: For small outer side
   - `BroadcastJoin`: Broadcast small table to all nodes (for distributed)
   - `ShuffleJoin`: Partition both sides on join key

3. **Exchange Operators**:
   - `Gather`: Collect data from remote source
   - `Broadcast`: Send data to multiple destinations
   - `Repartition`: Shuffle data by key

4. **Aggregate Strategies**:
   - `HashAggregate`: Hash-based grouping
   - `SortAggregate`: Sort-based grouping
   - `PartialAggregate`: Two-phase aggregation (local + global)

**Join Strategy Selection**:
```python
def select_join_strategy(left_card, right_card, left_source, right_source):
    # If one side is small (<10K rows), use broadcast
    if min(left_card, right_card) < 10000:
        return BroadcastJoin(broadcast_side=smaller_side)

    # If both on same data source, push down
    if left_source == right_source:
        return RemoteJoin(data_source=left_source)

    # If data sources are different and large, fetch both
    # Use hash join locally
    return HashJoin()
```

### 8. Execution Engine

**Purpose**: Execute physical plan and materialize results

**Components**:

1. **Executor**:
   - Tree-walking interpreter for physical plan
   - Iterator (pull) model: `next()` on each operator
   - Supports pipelining where possible

2. **Data Source Connectors**:
   - PostgreSQL connector (using psycopg2/asyncpg)
   - DuckDB connector (using duckdb-engine)
   - Abstraction for adding new sources

3. **Data Transfer**:
   - Arrow-based data transfer for efficiency
   - Streaming for large result sets
   - Parallel data fetching when possible

4. **Execution Strategies**:
   - **Remote Pushdown**: Send entire subplan to data source
   - **Local Execution**: Fetch data and execute locally
   - **Hybrid**: Push what we can, execute rest locally

**Example Execution Flow**:
```
Query: SELECT o.id, c.name FROM postgres.orders o
       JOIN duckdb.customers c ON o.customer_id = c.id
       WHERE o.amount > 1000

Physical Plan:
HashJoin (local)
├─ Gather (from PostgreSQL)
│  └─ RemoteScan: SELECT id, customer_id FROM orders WHERE amount > 1000
└─ Gather (from DuckDB)
   └─ RemoteScan: SELECT id, name FROM customers

Execution:
1. Push filter to PostgreSQL: "amount > 1000"
2. Fetch filtered orders into local memory (as Arrow table)
3. Fetch customers from DuckDB (as Arrow table)
4. Build hash table on customers.id
5. Probe with orders.customer_id
6. Return joined results
```

### 9. Statistics Collection

**Purpose**: Gather statistics for cost-based optimization

**Strategy**:
- **On-demand**: Collect stats when binding (ANALYZE tables)
- **Cached**: Cache statistics with TTL
- **Sampling**: Use sampling for large tables
- **Histograms**: Collect equi-width histograms for key columns

**Statistics Storage**:
```python
class TableStats:
    row_count: int
    total_size_bytes: int
    column_stats: Dict[str, ColumnStats]

class ColumnStats:
    num_distinct: int
    null_fraction: float
    avg_width: int
    histogram: Optional[Histogram]
    most_common_values: List[Tuple[Any, float]]
```

## Query Execution Pipeline

```
SQL Input
   ↓
[1. Parse] (sqlglot)
   ↓
Abstract Syntax Tree (AST)
   ↓
[2. Bind] (resolve references, types)
   ↓
Logical Plan (unoptimized)
   ↓
[3. Pre-Optimize] (constant folding, simplification)
   ↓
[4. Decorrelate] (remove correlated subqueries)
   ↓
[5. Logical Optimize] (pushdown, reordering)
   ↓
Optimized Logical Plan
   ↓
[6. Generate Physical Plans] (enumerate alternatives)
   ↓
Candidate Physical Plans
   ↓
[7. Cost Estimation] (choose best plan)
   ↓
Selected Physical Plan
   ↓
[8. Execute] (fetch data, compute results)
   ↓
Results (Arrow format)
```

## Key Design Decisions

### 1. Immutable Plan Nodes
- Plans are immutable for easier reasoning and thread safety
- Transformations create new plan nodes
- Enables plan caching and reuse

### 2. Rule-Based + Cost-Based Optimization
- Rule-based for deterministic transformations (pushdown, pruning)
- Cost-based for choosing between alternatives (join order, join method)
- Multiple optimization passes with fixed-point iteration

### 3. Volcano/Cascades-Style Optimizer
- Top-down planning with memoization
- Enumerate logical expressions in equivalence classes
- Generate physical plans for each logical expression
- Dynamic programming to find optimal plan

### 4. Arrow-Based Data Transfer
- Use Apache Arrow for in-memory columnar data
- Zero-copy data sharing between components
- Efficient serialization for network transfer
- Compatible with many data sources

### 5. Push-Down First Philosophy
- Always try to push computation to data sources
- Reduces data transfer (most expensive operation)
- Leverage remote database optimizers
- Fall back to local execution when necessary

### 6. Adaptive Query Execution (Future)
- Collect runtime statistics during execution
- Re-optimize if estimates were wrong
- Switch join strategies dynamically

## Performance Considerations

### 1. Data Movement Minimization
- **Problem**: Network transfer is expensive
- **Solution**: Aggressive pushdown, use partial aggregation, fetch only needed columns

### 2. Join Strategy Selection
- **Broadcast Join**: For small tables (< 10K rows or < 10MB)
- **Shuffle Join**: For large tables with good partitioning key
- **Semi-Join Pushdown**: Send join keys to reduce data fetched

### 3. Parallel Execution
- Fetch from multiple data sources in parallel
- Pipeline operators where possible
- Parallel hash join build/probe

### 4. Caching
- Cache statistics with TTL
- Cache small dimension tables
- Materialize expensive intermediate results

## Testing Strategy

1. **Unit Tests**: Each optimizer rule, cost estimation, plan generation
2. **Integration Tests**: End-to-end query execution with known results
3. **Performance Tests**: Query latency and throughput benchmarks
4. **Correctness Tests**: Compare results with single-database execution
5. **Fuzzing**: Generate random queries and verify correctness

## Future Enhancements

1. **Query Compilation**: JIT compile hot paths using LLVM
2. **Vectorization**: SIMD execution for expressions
3. **Distributed Execution**: Multi-node coordinator/worker architecture
4. **Advanced Statistics**: ML-based cardinality estimation
5. **Query Caching**: Cache query results with invalidation
6. **More Data Sources**: MySQL, SQLite, REST APIs, etc.
7. **Write Operations**: INSERT, UPDATE, DELETE across sources
8. **Transactions**: Distributed transactions with 2PC
9. **Views and Materialized Views**: Virtual tables and caching
10. **Security**: Row-level security, data masking, encryption

## References

- **Papers**:
  - "Volcano - An Extensible and Parallel Query Evaluation System" (Graefe, 1994)
  - "The Cascades Framework for Query Optimization" (Graefe, 1995)
  - "Query Optimization in Database Systems" (Chaudhuri, 1998)
  - "Unnesting Arbitrary Queries" (Neumann & Kemper, 2015)

- **Systems**:
  - PostgreSQL query planner
  - Apache Calcite (federated query framework)
  - DuckDB optimizer
  - Presto/Trino distributed query engine

---

This plan provides a solid foundation for a production-quality federated query engine. The architecture is modular, extensible, and follows proven database system design principles.
