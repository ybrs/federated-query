# LIMIT/OFFSET/ORDER BY Pushdown Rules

## Overview

This document defines the comprehensive rules and implementation strategy for pushing down LIMIT, OFFSET, and ORDER BY clauses to remote data sources in the federated query engine. This is a critical optimization that significantly reduces data transfer and improves query performance.

## Current State Analysis

### What Exists
- **Logical Plan**: `Sort` and `Limit` nodes exist in `federated_query/plan/logical.py`
- **Physical Plan**: `PhysicalSort` and `PhysicalLimit` nodes exist in `federated_query/plan/physical.py`
- **Scan Node Support**: `Scan` node already has `limit` and `offset` fields (lines 87-88 in logical.py)
- **PhysicalScan Execution**: `PhysicalScan._build_query()` already generates `LIMIT` and `OFFSET` SQL (lines 103-106 in physical.py)
- **Limit Pushdown Optimization**: `LimitPushdownRule` exists and pushes LIMIT through projections and filters to scans (lines 690-896 in rules.py)
- **Data Source Capabilities**: `DataSourceCapability.LIMIT` and `DataSourceCapability.ORDER_BY` are defined (lines 20-21 in datasources/base.py)
- **E2E Tests**: Comprehensive ORDER BY pushdown tests exist in `tests/e2e_pushdown/test_order_by_comprehensive.py`

### What's Missing
- **Parser Support**: Parser does NOT convert ORDER BY clauses from SQL to `Sort` nodes
  - No `_build_order_by_clause()` method in parser.py
  - ORDER BY clauses in sqlglot AST are being ignored
- **Physical Planner**: No handling of `Sort` nodes (not in `_plan_node()` dispatch)
- **PhysicalSort Execution**: `execute()` method raises `NotImplementedError` (line 1489 in physical.py)
- **Order By Pushdown Rule**: No optimizer rule to push `Sort` into `Scan` nodes
- **Scan Order By Storage**: `Scan` node has NO fields for storing ORDER BY information
- **PhysicalScan Query Building**: `_build_query()` does NOT generate ORDER BY SQL

## Problem Statement

A federated query engine must minimize data transfer from remote sources. Without pushdown:
1. **ORDER BY without pushdown**: Fetch entire table, sort locally
2. **LIMIT without ORDER BY**: Cannot safely push down (results are non-deterministic)
3. **ORDER BY + LIMIT**: Most critical case - must push both together to avoid fetching entire table

## Core Principles

### 1. Safety First
- **NEVER** push down operations that change query semantics
- **ALWAYS** verify data source capabilities before pushdown
- **PRESERVE** NULL handling semantics (NULLS FIRST/LAST)
- **MAINTAIN** sort stability when required

### 2. ORDER BY Pushdown is MANDATORY
- **ORDER BY pushdown is REQUIRED for correctness, not optional**
- **Many use cases depend on ORDER BY without LIMIT**:
  - Opening cursors for sequential processing (timeseries data)
  - Streaming results in deterministic order
  - Processing data in chronological/sorted order
  - Merge operations requiring sorted inputs
- **ORDER BY alone MUST be pushed down** when query has ORDER BY clause
- **LIMIT + ORDER BY together** provide additional optimization (reduce rows transferred)
- **LIMIT ALONE is unsafe for pushdown in multi-node scenarios** (non-deterministic results)

### 3. Pushdown Boundaries
- **Can push through**: `Project`, `Filter` (if all sort columns are available)
- **Cannot push through**: `Join`, `Aggregate`, `Union` (changes row order or aggregates data)
- **Target**: Always push to `Scan` when possible

## Detailed Rules

### Rule 1: ORDER BY Pushdown

#### 1.1 Basic ORDER BY Pushdown
**Condition**: Single table query with ORDER BY
**Action**: Push ORDER BY into Scan node
**Example**:
```sql
SELECT name, price FROM products ORDER BY price DESC
```
**Transformation**:
```
Before: Sort(Scan(products))
After:  Scan(products, order_by=[price DESC])
```

#### 1.2 ORDER BY Through Projection
**Condition**: ORDER BY columns are all in projection or available from input
**Action**: Push ORDER BY below projection if all sort columns available in input
**Example**:
```sql
SELECT name, price FROM products ORDER BY price
```
**Transformation**:
```
Before: Sort(Project(Scan(products)))
After:  Project(Scan(products, order_by=[price]))
```

#### 1.3 ORDER BY Through Filter
**Condition**: ORDER BY columns available after filter
**Action**: Always safe to push ORDER BY below filter
**Example**:
```sql
SELECT name, price FROM products WHERE category='electronics' ORDER BY price
```
**Transformation**:
```
Before: Sort(Filter(Scan(products)))
After:  Filter(Scan(products, order_by=[price]))
```

#### 1.4 ORDER BY Cannot Push Through Join
**Condition**: ORDER BY on join result
**Action**: Keep ORDER BY above join, cannot push to either scan
**Example**:
```sql
SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id=c.id ORDER BY o.id
```
**Transformation**: NO CHANGE
```
Sort(Join(Scan(orders), Scan(customers)))
```
**Rationale**: Join reorders rows, ORDER BY must execute after join completion

#### 1.5 ORDER BY Cannot Push Through Aggregate
**Condition**: ORDER BY on aggregate result
**Action**: Keep ORDER BY above aggregate
**Example**:
```sql
SELECT category, COUNT(*) cnt FROM products GROUP BY category ORDER BY cnt
```
**Transformation**: NO CHANGE
```
Sort(Aggregate(Scan(products)))
```
**Rationale**: Aggregate produces different rows, ORDER BY must execute on aggregated results

#### 1.6 Multi-Column ORDER BY
**Condition**: ORDER BY with multiple columns with mixed ASC/DESC
**Action**: Push all sort keys with their directions
**Example**:
```sql
SELECT * FROM orders ORDER BY region ASC, price DESC
```
**Transformation**:
```
Before: Sort(Scan(orders), keys=[region, price], dirs=[ASC, DESC])
After:  Scan(orders, order_by=[(region, ASC), (price, DESC)])
```

#### 1.7 ORDER BY with NULLS FIRST/LAST
**Condition**: Explicit NULL ordering specified
**Action**: Push NULL ordering specification to data source
**Example**:
```sql
SELECT * FROM orders ORDER BY region NULLS FIRST
```
**Transformation**:
```
After: Scan(orders, order_by=[(region, ASC, NULLS_FIRST)])
```
**Note**: Verify data source supports NULLS FIRST/LAST via capability check

### Rule 2: LIMIT/OFFSET Pushdown

#### 2.1 LIMIT + OFFSET Without ORDER BY (Simple Scan)
**Condition**: Single table scan, no joins/aggregates in query tree
**Action**: Push to scan (WITH WARNING - results are non-deterministic)
**Example**:
```sql
SELECT * FROM products LIMIT 10 OFFSET 20
```
**Transformation**:
```
After: Scan(products, limit=10, offset=20)
```
**Warning**: Results depend on physical storage order and may vary across executions

#### 2.2 LIMIT + OFFSET With ORDER BY
**Condition**: LIMIT/OFFSET applied to sorted results
**Action**: **MUST** push ORDER BY first, then push LIMIT/OFFSET (correctness requirement)
**Example**:
```sql
SELECT * FROM products ORDER BY price LIMIT 10 OFFSET 20
```
**Transformation**:
```
Before: Limit(Sort(Scan(products)), limit=10, offset=20)
After:  Scan(products, order_by=[price], limit=10, offset=20)
```
**Rationale**: This is the most critical optimization - fetches only 30 rows instead of entire table

#### 2.3 LIMIT Through Projection
**Condition**: LIMIT above projection
**Action**: Push LIMIT through projection to scan
**Example**:
```sql
SELECT name, price FROM products LIMIT 10
```
**Transformation**:
```
Before: Limit(Project(Scan(products)))
After:  Project(Scan(products, limit=10))
```

#### 2.4 LIMIT Through Filter
**Condition**: LIMIT above filter
**Action**: Push LIMIT through filter to scan
**Example**:
```sql
SELECT * FROM products WHERE price > 100 LIMIT 10
```
**Transformation**:
```
Before: Limit(Filter(Scan(products)))
After:  Filter(Scan(products, limit=10))
```
**Note**: Remote database applies limit AFTER filter, fetching up to 10 matching rows

#### 2.5 LIMIT Cannot Push Through Join
**Condition**: LIMIT on join result
**Action**: Keep LIMIT above join, do NOT push to child scans
**Example**:
```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id=c.id LIMIT 10
```
**Transformation**: NO CHANGE
```
Limit(Join(Scan(orders), Scan(customers)))
```
**Rationale**: Pushing limit to either side would change join cardinality and produce wrong results

#### 2.6 LIMIT Cannot Push Through Aggregate
**Condition**: LIMIT on aggregate result
**Action**: Keep LIMIT above aggregate
**Example**:
```sql
SELECT category, COUNT(*) FROM products GROUP BY category LIMIT 5
```
**Transformation**: NO CHANGE
```
Limit(Aggregate(Scan(products)))
```
**Rationale**: Must aggregate all groups first, then limit the number of groups

#### 2.7 LIMIT Cannot Push Through UNION
**Condition**: LIMIT on UNION result
**Action**: Keep LIMIT above union
**Example**:
```sql
(SELECT * FROM products) UNION (SELECT * FROM archived_products) LIMIT 10
```
**Transformation**: NO CHANGE
```
Limit(Union(Scan(products), Scan(archived_products)))
```
**Rationale**: Union combines results, limit applies to combined set

#### 2.8 OFFSET Composition
**Condition**: Multiple OFFSET values in plan (e.g., scan already has offset, new limit has offset)
**Action**: Add offsets together
**Example**:
```sql
-- If Scan already has offset=5, and we push Limit with offset=10
```
**Transformation**:
```
effective_offset = scan.offset + limit.offset  // 15
```

#### 2.9 LIMIT Composition
**Condition**: Multiple LIMIT values in plan (e.g., nested subqueries with limits)
**Action**: Take minimum of limits
**Example**:
```sql
-- If Scan already has limit=20, and we push Limit with limit=10
```
**Transformation**:
```
effective_limit = min(scan.limit, new_limit)  // 10
```

### Rule 3: Combined ORDER BY + LIMIT + OFFSET Pushdown

#### 3.1 Full Pushdown (Ideal Case)
**Condition**: Single table with ORDER BY + LIMIT + OFFSET, no joins/aggregates
**Action**: Push all three together to scan
**Example**:
```sql
SELECT * FROM orders ORDER BY order_date DESC LIMIT 100 OFFSET 50
```
**Transformation**:
```
Before: Limit(Sort(Scan(orders)), limit=100, offset=50)
After:  Scan(orders, order_by=[order_date DESC], limit=100, offset=50)
```
**Impact**: Fetch 150 rows instead of entire table

#### 3.2 ORDER BY + LIMIT Through Filter and Projection
**Condition**: Complex query with filters and projections
**Action**: Push ORDER BY and LIMIT through transparent operators
**Example**:
```sql
SELECT order_id, total
FROM orders
WHERE status='completed'
ORDER BY total DESC
LIMIT 10
```
**Transformation**:
```
Before: Limit(Sort(Project(Filter(Scan(orders)))))
After:  Project(Filter(Scan(orders, order_by=[total DESC], limit=10)))
```

#### 3.3 Partial Pushdown (ORDER BY Stops at Join)
**Condition**: ORDER BY column from one side of join
**Action**: Cannot push through join boundary
**Example**:
```sql
SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.customer_id=c.id
ORDER BY o.order_date
LIMIT 10
```
**Transformation**: NO CHANGE
```
Limit(Sort(Join(Scan(orders), Scan(customers))))
```
**Rationale**: ORDER BY must execute on join results

### Rule 4: Data Source Capability Checks

#### 4.1 Verify LIMIT Support
**Check**: `datasource.supports_capability(DataSourceCapability.LIMIT)`
**Action**: Only push LIMIT if data source supports it
**Fallback**: If not supported, execute LIMIT locally with PhysicalLimit

#### 4.2 Verify ORDER BY Support
**Check**: `datasource.supports_capability(DataSourceCapability.ORDER_BY)`
**Action**: Only push ORDER BY if data source supports it
**Fallback**: If not supported, execute ORDER BY locally with PhysicalSort

#### 4.3 Dialect-Specific Syntax
**Issue**: Different databases have different ORDER BY syntax
- PostgreSQL: `ORDER BY col NULLS FIRST`
- MySQL: `ORDER BY col ASC` (no NULLS FIRST)
- DuckDB: `ORDER BY col NULLS LAST`

**Action**: Use data source's dialect when generating SQL

### Rule 5: Edge Cases

#### 5.1 ORDER BY on Computed Columns
**Condition**: ORDER BY on expression not in table
**Action**: Cannot push to scan if expression needs computation
**Example**:
```sql
SELECT price, price * 1.1 as price_with_tax FROM products ORDER BY price_with_tax
```
**Transformation**: Keep Sort above Scan
```
Sort(Project(Scan(products)))
```
**Exception**: If data source supports computed ORDER BY expressions, can push the expression

#### 5.2 ORDER BY Position Reference
**Condition**: ORDER BY 1 (position reference)
**Action**: Resolve to actual column name before pushdown
**Example**:
```sql
SELECT name, price FROM products ORDER BY 2
```
**Resolve**: ORDER BY 2 → ORDER BY price
**Then push**: `Scan(products, order_by=[price])`

#### 5.3 OFFSET Without LIMIT
**Condition**: Query has OFFSET but no LIMIT
**Action**: Push OFFSET to scan
**Example**:
```sql
SELECT * FROM products OFFSET 100
```
**Transformation**:
```
After: Scan(products, offset=100)
```
**Note**: Fetches all rows after first 100

#### 5.4 LIMIT 0
**Condition**: LIMIT 0 (only want schema, no data)
**Action**: Push to scan
**Example**:
```sql
SELECT * FROM products LIMIT 0
```
**Transformation**:
```
After: Scan(products, limit=0)
```

#### 5.5 Negative LIMIT or OFFSET
**Condition**: Invalid negative values
**Action**: Reject at parse time with error
**Example**:
```sql
SELECT * FROM products LIMIT -1  -- ERROR
```

## Implementation Plan

### Phase 1: Add ORDER BY to Scan Node

**Changes Required**:

1. **Update Logical Scan** (`federated_query/plan/logical.py:Scan`)
   ```python
   @dataclass(frozen=True)
   class Scan(LogicalPlanNode):
       # ... existing fields ...
       limit: Optional[int] = None
       offset: int = 0
       # ADD:
       order_by: Optional[List[Tuple[Expression, bool]]] = None  # [(expr, ascending), ...]
       order_nulls: Optional[List[Optional[str]]] = None  # ["FIRST", "LAST", None, ...]
   ```

2. **Update Physical Scan** (`federated_query/plan/physical.py:PhysicalScan`)
   ```python
   @dataclass
   class PhysicalScan(PhysicalPlanNode):
       # ... existing fields ...
       limit: Optional[int] = None
       offset: int = 0
       # ADD:
       order_by: Optional[List[Tuple[Expression, bool]]] = None
       order_nulls: Optional[List[Optional[str]]] = None
   ```

3. **Update PhysicalScan._build_query()** (line 89 in physical.py)
   ```python
   def _build_query(self) -> str:
       cols = self._format_columns()
       table_ref = self._format_table_ref()
       query = f"SELECT {cols} FROM {table_ref}"

       if self.filters:
           where_clause = self.filters.to_sql()
           query = f"{query} WHERE {where_clause}"

       if self.group_by:
           group_clause = self._format_group_by()
           query = f"{query} GROUP BY {group_clause}"

       # ADD ORDER BY SUPPORT:
       if self.order_by:
           order_clause = self._format_order_by()
           query = f"{query} ORDER BY {order_clause}"

       if self.limit is not None:
           query = f"{query} LIMIT {self.limit}"
           if self.offset:
               query = f"{query} OFFSET {self.offset}"

       return query

   def _format_order_by(self) -> str:
       """Format ORDER BY clause for SQL."""
       items = []
       for i, (expr, ascending) in enumerate(self.order_by):
           item = expr.to_sql()
           if not ascending:
               item = f"{item} DESC"
           # Add NULLS FIRST/LAST if specified
           if self.order_nulls and i < len(self.order_nulls) and self.order_nulls[i]:
               item = f"{item} NULLS {self.order_nulls[i]}"
           items.append(item)
       return ", ".join(items)
   ```

### Phase 2: Add ORDER BY Parsing

**Changes Required**:

1. **Add to Parser** (`federated_query/parser/parser.py`)
   ```python
   def _convert_select(self, select: exp.Select) -> LogicalPlanNode:
       plan = self._build_from_clause(select)
       plan = self._build_where_clause(select, plan)
       plan = self._build_group_by_clause(select, plan)
       plan = self._build_having_clause(select, plan)
       plan = self._build_select_clause(select, plan)
       # ADD:
       plan = self._build_order_by_clause(select, plan)
       plan = self._build_limit_clause(select, plan)
       return plan

   def _build_order_by_clause(
       self, select: exp.Select, input_plan: LogicalPlanNode
   ) -> LogicalPlanNode:
       """Build Sort node from ORDER BY clause."""
       order = select.args.get("order")
       if not order:
           return input_plan

       sort_keys = []
       ascending = []
       nulls_order = []

       for ordered in order.expressions:
           # Extract the expression being ordered
           expr = self._convert_expression(ordered.this)
           sort_keys.append(expr)

           # Extract direction (ASC/DESC)
           is_desc = ordered.args.get("desc", False)
           ascending.append(not is_desc)

           # Extract NULLS FIRST/LAST
           nulls = ordered.args.get("nulls_first")
           if nulls is not None:
               nulls_order.append("FIRST" if nulls else "LAST")
           else:
               nulls_order.append(None)

       return Sort(
           input=input_plan,
           sort_keys=sort_keys,
           ascending=ascending,
           nulls_order=nulls_order  # Add this field to Sort node
       )
   ```

2. **Update Sort Node** (`federated_query/plan/logical.py:Sort`)
   ```python
   @dataclass(frozen=True)
   class Sort(LogicalPlanNode):
       input: LogicalPlanNode
       sort_keys: List[Expression]
       ascending: List[bool]
       # ADD:
       nulls_order: Optional[List[Optional[str]]] = None  # ["FIRST", "LAST", None, ...]
   ```

### Phase 3: Add ORDER BY Pushdown Rule

**Changes Required**:

1. **New Optimizer Rule** (`federated_query/optimizer/rules.py`)
   ```python
   class OrderByPushdownRule(OptimizationRule):
       """Push ORDER BY clauses to data sources when safe."""

       def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
           return self._push_order_by(plan)

       def _push_order_by(self, plan: LogicalPlanNode) -> LogicalPlanNode:
           """Recursively push ORDER BY down the plan tree."""
           if isinstance(plan, Sort):
               return self._try_push_sort(plan)

           # Recurse into children for other node types
           return self._recurse_children(plan)

       def _try_push_sort(self, sort: Sort) -> LogicalPlanNode:
           """Try to push sort into its input."""
           input_node = sort.input

           # Can push through Project if all sort keys available in input
           if isinstance(input_node, Project):
               return self._push_through_project(sort, input_node)

           # Can push through Filter (always safe)
           if isinstance(input_node, Filter):
               return self._push_through_filter(sort, input_node)

           # Can push directly to Scan
           if isinstance(input_node, Scan):
               return self._push_to_scan(sort, input_node)

           # Cannot push through Join, Aggregate, Union
           # Keep Sort above these operators
           return sort

       def _push_to_scan(self, sort: Sort, scan: Scan) -> Scan:
           """Push ORDER BY into scan node."""
           order_by = list(zip(sort.sort_keys, sort.ascending))

           return Scan(
               datasource=scan.datasource,
               schema_name=scan.schema_name,
               table_name=scan.table_name,
               columns=scan.columns,
               filters=scan.filters,
               alias=scan.alias,
               group_by=scan.group_by,
               aggregates=scan.aggregates,
               output_names=scan.output_names,
               limit=scan.limit,
               offset=scan.offset,
               order_by=order_by,
               order_nulls=sort.nulls_order,
           )

       def _push_through_project(
           self, sort: Sort, project: Project
       ) -> LogicalPlanNode:
           """Push ORDER BY through projection if all sort columns available."""
           # Check if all sort columns are available in projection input
           input_cols = self._get_available_columns(project.input)
           sort_cols = self._extract_sort_columns(sort)

           if not sort_cols.issubset(input_cols):
               # Cannot push - sort columns not all available in input
               # Keep Sort above Project
               return sort

           # Push Sort below Project
           new_input = self._push_order_by(project.input)
           if isinstance(new_input, Scan):
               # Successfully pushed to scan, wrap with project
               new_scan = self._push_to_scan(sort, new_input)
               return Project(new_scan, project.expressions, project.aliases)

           # Could not push to scan, keep original structure
           return sort

       def _push_through_filter(
           self, sort: Sort, filter_node: Filter
       ) -> LogicalPlanNode:
           """Push ORDER BY through filter (always safe)."""
           # Recurse into filter's input
           new_input = sort.input.input  # Get filter's input

           if isinstance(new_input, Scan):
               new_scan = self._push_to_scan(sort, new_input)
               return Filter(new_scan, filter_node.predicate)

           # Keep original if can't push further
           return sort

       def name(self) -> str:
           return "OrderByPushdown"
   ```

2. **Register Rule** in RuleBasedOptimizer initialization
   ```python
   # In query executor or optimizer setup:
   optimizer.add_rule(OrderByPushdownRule())
   ```

### Phase 4: Update Physical Planner

**Changes Required**:

1. **Add Sort Planning** (`federated_query/optimizer/physical_planner.py`)
   ```python
   def _plan_node(self, node: LogicalPlanNode) -> PhysicalPlanNode:
       if isinstance(node, Scan):
           return self._plan_scan(node)
       # ... existing cases ...
       # ADD:
       if isinstance(node, Sort):
           return self._plan_sort(node)
       # ... rest ...

   def _plan_sort(self, sort: Sort) -> PhysicalPlanNode:
       """Plan a sort node."""
       input_plan = self._plan_node(sort.input)

       # Check if input is a scan with ORDER BY capability
       if isinstance(input_plan, PhysicalScan):
           # Check if datasource supports ORDER BY
           datasource = input_plan.datasource_connection
           if datasource and datasource.supports_capability(
               DataSourceCapability.ORDER_BY
           ):
               # ORDER BY was pushed down, no need for PhysicalSort
               return input_plan

       # Need to sort locally
       return PhysicalSort(
           input=input_plan,
           sort_keys=sort.sort_keys,
           ascending=sort.ascending,
       )

   def _plan_scan(self, scan: Scan) -> PhysicalScan:
       """Plan a scan node."""
       datasource = self.catalog.get_datasource(scan.datasource)
       if datasource is None:
           raise ValueError(f"Data source not found: {scan.datasource}")

       return PhysicalScan(
           datasource=scan.datasource,
           schema_name=scan.schema_name,
           table_name=scan.table_name,
           columns=scan.columns,
           filters=scan.filters,
           datasource_connection=datasource,
           group_by=scan.group_by,
           aggregates=scan.aggregates,
           output_names=scan.output_names,
           alias=scan.alias,
           limit=scan.limit,
           offset=scan.offset,
           # ADD:
           order_by=scan.order_by,
           order_nulls=scan.order_nulls,
       )
   ```

### Phase 5: Implement PhysicalSort (Fallback)

**Changes Required**:

1. **Implement PhysicalSort.execute()** (`federated_query/plan/physical.py`)
   ```python
   def execute(self) -> Iterator[pa.RecordBatch]:
       """Execute sort by materializing input and sorting."""
       # Materialize all input batches
       batches = []
       for batch in self.input.execute():
           batches.append(batch)

       if not batches:
           return

       # Combine into single table
       table = pa.Table.from_batches(batches)

       # Build sort keys for PyArrow
       sort_keys = []
       for i, key_expr in enumerate(self.sort_keys):
           # Extract column name from expression
           col_name = self._extract_column_name(key_expr)
           direction = "ascending" if self.ascending[i] else "descending"
           sort_keys.append((col_name, direction))

       # Sort the table
       indices = pa.compute.sort_indices(
           table,
           sort_keys=sort_keys
       )
       sorted_table = table.take(indices)

       # Yield as batches
       for batch in sorted_table.to_batches():
           yield batch

   def _extract_column_name(self, expr: Expression) -> str:
       """Extract column name from expression."""
       from .expressions import ColumnRef
       if isinstance(expr, ColumnRef):
           return expr.column
       raise NotImplementedError(
           f"ORDER BY on complex expressions not yet supported: {expr}"
       )
   ```

### Phase 6: Combined ORDER BY + LIMIT Optimization

**Changes Required**:

1. **Update LimitPushdownRule** to handle ORDER BY + LIMIT together
   ```python
   class LimitPushdownRule(OptimizationRule):
       # ... existing code ...

       def _try_push_limit(self, limit: Limit) -> LogicalPlanNode:
           """Try to push limit through input."""
           input_node = limit.input

           # Special case: Limit(Sort(Scan))
           # This is the most important optimization
           if isinstance(input_node, Sort):
               return self._push_limit_with_sort(limit, input_node)

           # ... existing logic for other cases ...

       def _push_limit_with_sort(
           self, limit: Limit, sort: Sort
       ) -> LogicalPlanNode:
           """Push LIMIT with ORDER BY together to scan."""
           # First, try to push Sort
           sort_input = sort.input

           # Can only push if sort goes to scan
           if isinstance(sort_input, Scan):
               # Push both ORDER BY and LIMIT to scan
               return Scan(
                   datasource=sort_input.datasource,
                   schema_name=sort_input.schema_name,
                   table_name=sort_input.table_name,
                   columns=sort_input.columns,
                   filters=sort_input.filters,
                   alias=sort_input.alias,
                   group_by=sort_input.group_by,
                   aggregates=sort_input.aggregates,
                   output_names=sort_input.output_names,
                   # Push both together:
                   order_by=list(zip(sort.sort_keys, sort.ascending)),
                   order_nulls=sort.nulls_order,
                   limit=limit.limit,
                   offset=limit.offset,
               )

           # Can't push both, keep original structure
           return limit
   ```

### Phase 7: Testing

**Test Coverage Required**:

1. **Unit Tests**: Test each pushdown rule in isolation
2. **Integration Tests**: Test parser → optimizer → planner → execution
3. **E2E Tests**: Already exist in `tests/e2e_pushdown/test_order_by_comprehensive.py`
4. **Edge Cases**: Test all edge cases listed in Rule 5

## Expected Performance Impact

### Benchmark Scenarios

| Query Pattern | Without Pushdown | With Pushdown | Improvement | Notes |
|---------------|------------------|---------------|-------------|-------|
| ORDER BY only | Fetch all rows, sort locally | Fetch all rows, sorted remotely | Critical for correctness | Required for cursors, timeseries |
| LIMIT only | Fetch all rows, limit locally | Fetch N rows | 100-1000x | Unsafe without ORDER BY |
| ORDER BY + LIMIT | Fetch all rows, sort, limit | Fetch N rows, sorted | 1000-10000x | Most critical optimization |
| ORDER BY + LIMIT + Filter | Fetch all rows, filter, sort, limit | Remote filter, sort, limit | 1000-10000x | Common pattern |

**Critical Cases**:

1. **ORDER BY for cursors/timeseries** (MANDATORY):
   ```sql
   SELECT * FROM events ORDER BY timestamp
   -- Used by: cursor processing, streaming, chronological order
   ```
   - Without pushdown: Database returns unsorted → client gets wrong order
   - With pushdown: Database returns properly sorted → correct behavior
   - **Impact**: Correctness requirement, not optimization

2. **ORDER BY + LIMIT for pagination** (OPTIMIZATION):
   ```sql
   SELECT * FROM large_table ORDER BY col LIMIT 10
   ```
   - Without pushdown: Fetch 1,000,000 rows, sort locally, take 10
   - With pushdown: Fetch 10 rows pre-sorted
   - **Impact**: 100,000x reduction in data transfer

## Migration Notes

### Backward Compatibility
- All existing queries continue to work
- No breaking changes to API
- New optimization is transparent to users

### Configuration
- Add configuration options to enable/disable specific pushdown rules
- Allow per-data-source capability overrides

### Monitoring
- Add metrics for pushdown effectiveness:
  - Number of queries with ORDER BY pushdown
  - Number of queries with LIMIT pushdown
  - Data transfer reduction

## Summary

This implementation plan provides:
1. **Comprehensive rules** for when pushdown is safe and beneficial
2. **Detailed implementation steps** for each phase
3. **Edge case handling** for robustness
4. **Performance expectations** to validate success

The most critical optimization is **ORDER BY + LIMIT pushdown together**, which can provide 1000-10000x performance improvement for common pagination queries.

All implementation must follow the project's coding standards:
- No bare exception catching
- No list comprehensions (use explicit loops)
- Functions under 20 lines and cyclomatic complexity under 4
- PEP-8 compliance
- Production-quality, real query engine behavior
