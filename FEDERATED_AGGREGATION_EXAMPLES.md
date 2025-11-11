# Federated Aggregation Examples - Documentation

## Overview

The `example/aggregate_queries.py` script demonstrates **Phase 3 aggregation capabilities** with federated data sources, showcasing real-world analytics scenarios across PostgreSQL and DuckDB.

## What's Demonstrated

### 1. Federated Data Setup
- **PostgreSQL**: `orders` table (order_id, product_id, region, quantity, order_date)
- **DuckDB**: `products` table (product_id, product_name, category, unit_price)
- **Relationship**: Orders reference products via product_id

### 2. Three Sections of Examples

#### Section 1: Single-Source Aggregations (DuckDB)
Basic aggregation functionality on a single datasource.

**Example Queries:**
- `COUNT(*)` - Total product count
- `GROUP BY category` with `COUNT, AVG, MIN, MAX` - Statistics by category

**Purpose:** Validate core Phase 3 functionality works correctly.

#### Section 2: Federated JOIN + Aggregation (PostgreSQL + DuckDB)
**This is the key demonstration** - combining Phase 2 (JOINs) and Phase 3 (Aggregations).

**Example Queries:**

1. **Orders by Product Category**
```sql
SELECT
    p.category,
    COUNT(*) as order_count,
    SUM(o.quantity) as total_quantity,
    AVG(o.quantity) as avg_quantity
FROM sales_db.public.orders o
JOIN analytics_db.main.products p ON o.product_id = p.product_id
GROUP BY p.category
```
Joins PostgreSQL orders with DuckDB products, then aggregates by category.

2. **Multi-Column GROUP BY**
```sql
SELECT
    o.region,
    p.category,
    COUNT(*) as order_count,
    SUM(o.quantity) as total_items
FROM sales_db.public.orders o
JOIN analytics_db.main.products p ON o.product_id = p.product_id
GROUP BY o.region, p.category
```
Groups by region (from PostgreSQL) and category (from DuckDB).

3. **Product Popularity Analysis**
```sql
SELECT
    p.product_name,
    COUNT(*) as times_ordered,
    SUM(o.quantity) as total_sold,
    AVG(o.quantity) as avg_order_size
FROM sales_db.public.orders o
JOIN analytics_db.main.products p ON o.product_id = p.product_id
GROUP BY p.product_name
```
Analyzes which products are most popular across all orders.

4. **Filtered Federated Aggregation**
```sql
SELECT
    o.region,
    COUNT(DISTINCT o.product_id) as unique_products,
    COUNT(*) as total_orders,
    SUM(o.quantity) as total_items
FROM sales_db.public.orders o
JOIN analytics_db.main.products p ON o.product_id = p.product_id
WHERE p.category = 'Electronics'
GROUP BY o.region
```
Combines WHERE filtering with federated JOIN and aggregation.

#### Section 3: Aggregation Pushdown (Future Optimization)
Documents **future optimization opportunities** with detailed examples.

## Aggregation Pushdown - The Future

### What is Aggregation Pushdown?

Currently, the engine:
1. Scans all rows from source databases
2. Transfers all data to the query engine
3. Performs JOIN and aggregation locally

With pushdown, the engine would:
1. Identify aggregations that can run at the source
2. Generate SQL to execute remotely (e.g., `SELECT region, COUNT(*) FROM orders GROUP BY region`)
3. Transfer only aggregated results (much smaller!)
4. Perform final aggregation if needed

### Three Pushdown Scenarios

#### 1. Single-Source Aggregation Pushdown

**Query:**
```sql
SELECT region, COUNT(*), SUM(quantity)
FROM sales_db.public.orders
GROUP BY region
```

**Without pushdown:**
- Transfer: 10 rows
- Aggregation: Local

**With pushdown:**
- Execute at PostgreSQL: `SELECT region, COUNT(*), SUM(quantity) FROM orders GROUP BY region`
- Transfer: 4 rows (one per region)
- Aggregation: Remote

**Benefits:**
- 60% less network transfer
- Leverages PostgreSQL's optimized aggregation
- Lower memory in query engine

#### 2. Partial Aggregation Pushdown

**Query:**
```sql
SELECT p.category, SUM(o.quantity), COUNT(*)
FROM sales_db.public.orders o
JOIN analytics_db.main.products p ON o.product_id = p.product_id
GROUP BY p.category
```

**Without pushdown:**
- Scan 10 order rows from PostgreSQL
- Scan 5 product rows from DuckDB
- JOIN locally (10 rows)
- Aggregate locally

**With partial pushdown:**
- Execute at PostgreSQL: `SELECT product_id, SUM(quantity) as qty, COUNT(*) as cnt FROM orders GROUP BY product_id`
- Transfer: 5 aggregated rows instead of 10 raw rows
- JOIN locally (5 rows instead of 10)
- Final aggregation locally

**Benefits:**
- 50% less data from PostgreSQL
- Pre-aggregation reduces JOIN size
- Especially beneficial with high-cardinality joins

#### 3. Multi-Stage Pushdown

**Query:**
```sql
SELECT
    region_stats.region,
    region_stats.order_count,
    category_stats.category,
    category_stats.product_count
FROM
    (SELECT region, COUNT(*) as order_count
     FROM sales_db.public.orders
     GROUP BY region) region_stats
CROSS JOIN
    (SELECT category, COUNT(*) as product_count
     FROM analytics_db.main.products
     GROUP BY category) category_stats
```

Both aggregations can be pushed to their respective sources.

**Benefits:**
- Both datasources do aggregation work
- Minimal data transfer
- Highly parallel execution

## Implementation Requirements for Pushdown

To implement aggregation pushdown in Phase 4:

### 1. Logical Optimization Rule
```python
class AggregationPushdownRule:
    """Push aggregation to datasource when possible."""

    def can_pushdown(self, aggregate: Aggregate) -> bool:
        # Check if:
        # 1. Input is single-source scan
        # 2. Datasource supports aggregation
        # 3. All expressions can be pushed
        pass

    def rewrite(self, aggregate: Aggregate) -> LogicalPlanNode:
        # Replace Aggregate(Scan(...))
        # with RemoteAggregate(datasource, sql)
        pass
```

### 2. Cost-Based Decision Making
```python
class CostModel:
    def estimate_local_aggregation(self, plan):
        # Cost = scan_cost + network_transfer + local_agg_cost
        pass

    def estimate_remote_aggregation(self, plan):
        # Cost = remote_agg_cost + network_transfer(aggregated)
        pass
```

### 3. Partial Aggregation Support
```python
class PartialAggregator:
    """Handles partial pre-aggregation before JOIN."""

    def identify_pre_aggregate_opportunities(self, plan):
        # Find JOIN + Aggregate patterns
        # Determine if pre-aggregation is beneficial
        pass

    def merge_partial_results(self, partial_results):
        # Combine partial aggregates (e.g., SUM of SUMs)
        pass
```

### 4. Datasource Capability Detection
```python
class DataSourceCapabilities:
    supports_aggregation: bool
    supported_functions: List[str]  # COUNT, SUM, AVG, etc.
    supports_group_by: bool
    max_group_columns: Optional[int]
```

## Running the Examples

### With DuckDB Only (No PostgreSQL)
```bash
PYTHONPATH=/home/user/federated-query python example/aggregate_queries.py
```

Output:
- Section 1: Single-source aggregations ✓
- Section 2: Skipped (PostgreSQL not available)
- Section 3: Pushdown documentation ✓

### With PostgreSQL
```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run examples
PYTHONPATH=/home/user/federated-query python example/aggregate_queries.py
```

Output:
- Section 1: Single-source aggregations ✓
- Section 2: Federated JOIN + aggregation ✓ (4 queries)
- Section 3: Pushdown documentation ✓

## Key Takeaways

### What Works Now (Phase 3)
✅ **Federated aggregations** - JOIN data from different databases, then aggregate
✅ **All standard aggregate functions** - COUNT, SUM, AVG, MIN, MAX
✅ **Complex GROUP BY** - Single or multiple columns
✅ **Filtered aggregations** - WHERE + JOIN + GROUP BY
✅ **Production-ready** - Handles real-world analytics queries

### What's Coming (Phase 4)
⏳ **Aggregation pushdown** - Execute aggregations at source databases
⏳ **Partial pre-aggregation** - Reduce data transfer before JOINs
⏳ **Cost-based optimization** - Choose best aggregation strategy
⏳ **Multi-stage pushdown** - Push to multiple sources in parallel

## Performance Characteristics

### Current Implementation (No Pushdown)

**Simple aggregation:**
- Complexity: O(n) scan + O(n) hash aggregation
- Memory: O(groups) for hash table
- Network: Transfer all source rows

**Federated aggregation:**
- Complexity: O(n₁) scan source 1 + O(n₂) scan source 2 + O(n₁ + n₂) JOIN + O(result) aggregation
- Memory: O(groups) for aggregation + O(max(n₁, n₂)) for JOIN
- Network: Transfer all rows from both sources

### With Pushdown (Future)

**Single-source pushdown:**
- Complexity: Remote O(n) scan + aggregation, local O(1) if fully pushed
- Memory: O(groups) remote, minimal local
- Network: Transfer only aggregated rows (typically orders of magnitude smaller)

**Partial pushdown:**
- Complexity: Remote O(n) pre-aggregation, local O(pre_agg_rows) JOIN + final aggregation
- Memory: Reduced by pre-aggregation ratio
- Network: Transfer pre-aggregated rows (10-1000x reduction typical)

## Example Output

```
================================================================================
FEDERATED QUERY ENGINE - PHASE 3 AGGREGATION EXAMPLES
================================================================================

Setup complete:
  - DuckDB: products table (5 rows)
  - PostgreSQL: orders table (10 rows)

================================================================================
SECTION 1: SINGLE-SOURCE AGGREGATIONS
================================================================================

================================================================================
EXAMPLE: Simple COUNT - Total number of products
================================================================================
SQL:
        SELECT COUNT(*) as total_products
        FROM analytics_db.main.products

Results (1 rows):
pyarrow.Table
total_products: int64
----
total_products: [[5]]

================================================================================
SECTION 2: FEDERATED JOIN + AGGREGATION
================================================================================

================================================================================
EXAMPLE: Federated JOIN + GROUP BY - Orders by product category
================================================================================
SQL:
        SELECT
            p.category,
            COUNT(*) as order_count,
            SUM(o.quantity) as total_quantity,
            AVG(o.quantity) as avg_quantity
        FROM sales_db.public.orders o
        JOIN analytics_db.main.products p ON o.product_id = p.product_id
        GROUP BY p.category

Results (2 rows):
pyarrow.Table
category: string
order_count: int64
total_quantity: double
avg_quantity: double
----
category: [["Electronics","Hardware"]]
order_count: [[7,3]]
total_quantity: [[30,11]]
avg_quantity: [[4.285714285714286,3.6666666666666665]]
```

## Use Cases Demonstrated

1. **E-commerce Analytics**: Product sales by category
2. **Regional Analysis**: Orders and revenue by geographic region
3. **Inventory Planning**: Product popularity and demand patterns
4. **Cross-Database Reporting**: Combining transactional (PostgreSQL) with reference data (DuckDB)
5. **Filtered Analytics**: Category-specific regional analysis

## Conclusion

The federated aggregation examples demonstrate that **Phase 3 is production-ready** for complex analytics across heterogeneous data sources. The aggregation pushdown documentation provides a clear roadmap for Phase 4 performance optimizations.

**Current capabilities enable real-world federated analytics today.**
**Future optimizations will make it enterprise-scale performant.**
