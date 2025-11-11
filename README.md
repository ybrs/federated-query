# Federated Query Engine

A production-grade federated query engine written in Python that executes SQL queries across multiple heterogeneous data sources (PostgreSQL, DuckDB) in an optimal way.

## Features

- **Multi-Source Querying**: Execute SQL queries across PostgreSQL and DuckDB data sources
- **Intelligent Optimization**: Cost-based query optimization with predicate pushdown, projection pushdown, and join reordering
- **Decorrelation**: Automatic transformation of correlated subqueries into efficient joins
- **Flexible Execution**: Support for broadcast joins, hash joins, and nested loop joins
- **Arrow-Based**: Uses Apache Arrow for efficient in-memory data representation and transfer
- **Production-Ready**: Comprehensive error handling, logging, and configuration

## Architecture

The engine follows a classic database architecture with several optimization phases:

```
SQL Query
   ↓
[Parser] (sqlglot) → AST
   ↓
[Binder] → Resolved Logical Plan
   ↓
[Pre-Optimizer] → Simplified Plan
   ↓
[Decorrelator] → Decorrelated Plan
   ↓
[Logical Optimizer] → Optimized Logical Plan
   ↓
[Physical Planner] → Physical Plan (with cost estimation)
   ↓
[Executor] → Results (Arrow format)
```

## Project Structure

```
federated-query/
├── federated_query/          # Main package
│   ├── catalog/              # Metadata catalog
│   ├── config/               # Configuration management
│   ├── datasources/          # Data source connectors
│   │   ├── postgresql.py     # PostgreSQL connector
│   │   └── duckdb.py         # DuckDB connector
│   ├── executor/             # Query executor
│   ├── optimizer/            # Query optimization
│   │   ├── rules.py          # Optimization rules
│   │   ├── cost.py           # Cost model
│   │   ├── decorrelation.py  # Subquery decorrelation
│   │   └── statistics.py     # Statistics collection
│   ├── parser/               # SQL parser and binder
│   ├── plan/                 # Plan representations
│   │   ├── logical.py        # Logical plan nodes
│   │   ├── physical.py       # Physical plan nodes
│   │   └── expressions.py    # Expression nodes
│   └── utils/                # Utilities
├── config/                   # Configuration files
│   └── example_config.yaml   # Example configuration
├── tests/                    # Test suite
├── plan.md                   # Architecture documentation
└── tasks.md                  # Implementation roadmap
```

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd federated-query

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## Configuration

Create a YAML configuration file defining your data sources:

```yaml
datasources:
  postgres_prod:
    type: postgresql
    host: localhost
    port: 5432
    database: analytics
    user: postgres
    password: postgres
    schemas: [public]
    capabilities: [aggregations, joins, window_functions]

  local_duckdb:
    type: duckdb
    path: /data/analytics.duckdb
    read_only: true
    capabilities: [aggregations, joins]

optimizer:
  enable_predicate_pushdown: true
  enable_join_reordering: true

executor:
  max_memory_mb: 2048
  batch_size: 10000
```

See `config/example_config.yaml` for a complete example.

## Usage

```python
from federated_query.config import load_config
from federated_query.catalog import Catalog
from federated_query.parser import Parser, Binder
from federated_query.optimizer import RuleBasedOptimizer
from federated_query.executor import Executor

# Load configuration
config = load_config("config.yaml")

# Initialize catalog
catalog = Catalog()
# ... register data sources and load metadata ...

# Parse and execute query
parser = Parser()
binder = Binder(catalog)
optimizer = RuleBasedOptimizer(catalog)
executor = Executor(config.executor)

sql = """
    SELECT o.id, c.name, o.amount
    FROM postgres_prod.public.orders o
    JOIN local_duckdb.main.customers c ON o.customer_id = c.id
    WHERE o.amount > 1000
"""

# Parse → Bind → Optimize → Execute
ast = parser.parse(sql)
logical_plan = parser.ast_to_logical_plan(ast)
bound_plan = binder.bind(logical_plan)
optimized_plan = optimizer.optimize(bound_plan)
# ... generate physical plan ...
results = executor.execute_to_table(physical_plan)

print(results)
```

## Development Status

This project is under active development. See `tasks.md` for the implementation roadmap.

### Completed Phases

**Phase 0: Foundation** ✅
- ✅ Project structure and skeleton
- ✅ Core abstractions (plans, expressions, data sources)
- ✅ Configuration system
- ✅ Basic catalog structure
- ✅ Data source connectors (PostgreSQL, DuckDB)
- ✅ Test infrastructure

**Phase 1: Basic Query Execution** ✅
- ✅ Parser: AST to logical plan conversion
- ✅ Binder: Reference resolution with catalog integration
- ✅ Physical operators: Scan, Filter, Project, Limit
- ✅ Basic executor: Single-table queries
- ✅ End-to-end pipeline for simple SELECT queries

**Query Example (Phase 1):**
```sql
SELECT col1, col2 FROM datasource.schema.table WHERE col1 > 10 LIMIT 100
```

**Phase 2: Joins and Multi-Table Queries** ✅
- ✅ Logical Join plan node with all join types
- ✅ Physical HashJoin implementation
- ✅ Physical NestedLoopJoin implementation
- ✅ Parser support for JOIN clauses
- ✅ Binder support for multi-table column resolution
- ✅ Physical planner join strategy selection
- ✅ All 70 tests passing (including 5 join tests)
- ✅ Federated join example working (DuckDB + PostgreSQL)

**Query Example (Phase 2):**
```sql
SELECT c.name, o.order_id, o.amount
FROM duckdb.customers c
JOIN postgres.orders o ON c.id = o.customer_id
WHERE o.amount > 1000
```

**Phase 3: Aggregations and Grouping** ✅
- ✅ Aggregations and grouping
- ✅ GROUP BY clause support (single and multiple columns)
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ HAVING clause support with expression rewriting
- ✅ Global aggregations (without GROUP BY)
- ✅ Federated aggregations across data sources

**Query Example (Phase 3):**
```sql
SELECT c.name, COUNT(*) as order_count, SUM(o.amount) as total_spent
FROM duckdb.customers c
JOIN postgres.orders o ON c.id = o.customer_id
GROUP BY c.name
HAVING SUM(o.amount) > 2000
```

### Next Phases
- **Phase 4**: Pre-optimization and expression handling (constant folding, simplification)
- **Phase 5**: Statistics and cost model
- **Phase 6**: Logical optimization (predicate pushdown, join reordering)
- **Phase 7**: Subquery decorrelation
- **Phase 8**: Physical planning and advanced join strategies
- **Phase 9**: Parallel execution and memory management
- **Phase 10**: Advanced SQL features (ORDER BY, window functions, CTEs)

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=federated_query

# Run specific test file
pytest tests/test_parser.py
```

## Documentation

- **[plan.md](plan.md)**: Detailed architecture and design decisions
- **[tasks.md](tasks.md)**: Implementation roadmap broken down by phases

## Key Design Principles

1. **Immutable Plans**: All plan nodes are immutable for easier reasoning and thread safety
2. **Push-Down First**: Always try to push computation to data sources to minimize data transfer
3. **Arrow-Based**: Use Apache Arrow for efficient columnar data representation
4. **Cost-Based**: Use statistics and cost models to choose optimal execution strategies
5. **Extensible**: Easy to add new data sources, optimization rules, and operators

## Performance Considerations

- **Predicate Pushdown**: Filters are pushed to data sources whenever possible
- **Projection Pushdown**: Only required columns are fetched from sources
- **Join Strategy Selection**: Automatically chooses between broadcast, hash, and nested loop joins
- **Parallel Execution**: Fetches from multiple data sources in parallel
- **Memory Management**: Spills to disk when memory limits are exceeded

## Contributing

Contributions are welcome! Please see the implementation roadmap in `tasks.md` for areas that need work.

## License

MIT License

## References

### Papers
- "Volcano - An Extensible and Parallel Query Evaluation System" (Graefe, 1994)
- "The Cascades Framework for Query Optimization" (Graefe, 1995)
- "Unnesting Arbitrary Queries" (Neumann & Kemper, 2015)

### Systems
- PostgreSQL query planner
- Apache Calcite (federated query framework)
- DuckDB optimizer
- Presto/Trino distributed query engine
