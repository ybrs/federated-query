# Federated Query Engine - Claude Developer Guide

This document provides an overview of the codebase structure, key classes, compilation instructions, and coding standards for AI-assisted development.

## Repository Overview

This is a production-grade federated query engine that executes SQL queries across multiple heterogeneous data sources (PostgreSQL, DuckDB) with intelligent optimization.

## Project Structure

```
federated-query/
├── federated_query/          # Main package
│   ├── catalog/              # Metadata catalog
│   │   ├── catalog.py        # Catalog class - manages metadata across data sources
│   │   └── schema.py         # Schema, Table, Column classes
│   ├── config/               # Configuration management
│   │   └── config.py         # Config classes and YAML loader
│   ├── datasources/          # Data source connectors
│   │   ├── base.py           # DataSource abstract base class
│   │   ├── postgresql.py     # PostgreSQLDataSource implementation
│   │   └── duckdb.py         # DuckDBDataSource implementation
│   ├── executor/             # Query execution engine
│   │   ├── executor.py       # Executor class - executes physical plans
│   │   └── operators.py      # Physical operators (joins, aggregations, etc.)
│   ├── optimizer/            # Query optimization
│   │   ├── rules.py          # RuleBasedOptimizer and optimization rules
│   │   ├── cost.py           # CostModel for query cost estimation
│   │   ├── decorrelation.py  # Subquery decorrelation transformer
│   │   └── statistics.py     # Statistics collection and estimation
│   ├── parser/               # SQL parsing and binding
│   │   ├── parser.py         # Parser class - SQL to logical plan
│   │   └── binder.py         # Binder class - resolves references
│   ├── plan/                 # Plan representations
│   │   ├── logical.py        # Logical plan nodes (LogicalScan, LogicalJoin, etc.)
│   │   ├── physical.py       # Physical plan nodes (PhysicalScan, PhysicalHashJoin, etc.)
│   │   └── expressions.py    # Expression nodes (Column, Literal, BinaryOp, etc.)
│   └── utils/                # Utilities
│       └── logging.py        # Logging configuration
├── config/                   # Configuration files
│   └── example_config.yaml   # Example configuration
├── tests/                    # Test suite
│   ├── test_parser.py        # Parser tests
│   ├── test_catalog.py       # Catalog tests
│   ├── test_config.py        # Configuration tests
│   └── test_datasources.py   # Data source tests
├── scripts/                  # Utility scripts
│   └── init_duckdb.py        # Initialize test DuckDB database
└── docker/                   # Docker configuration for test databases
```

## Key Classes and Their Locations

### Core Query Pipeline
- **Parser** (`federated_query/parser/parser.py`): Converts SQL to logical plan using sqlglot
- **Binder** (`federated_query/parser/binder.py`): Resolves table/column references against catalog
- **RuleBasedOptimizer** (`federated_query/optimizer/rules.py`): Applies optimization rules to logical plan
- **Executor** (`federated_query/executor/executor.py`): Executes physical plan and returns results

### Metadata Management
- **Catalog** (`federated_query/catalog/catalog.py`): Central metadata registry
- **Schema** (`federated_query/catalog/schema.py`): Schema metadata container
- **Table** (`federated_query/catalog/schema.py`): Table metadata with columns
- **Column** (`federated_query/catalog/schema.py`): Column metadata with type info

### Data Source Connectors
- **DataSource** (`federated_query/datasources/base.py`): Abstract base class for all connectors
- **PostgreSQLDataSource** (`federated_query/datasources/postgresql.py`): PostgreSQL connector with pooling
- **DuckDBDataSource** (`federated_query/datasources/duckdb.py`): DuckDB connector

### Plan Nodes
- **Logical Plans** (`federated_query/plan/logical.py`): LogicalScan, LogicalJoin, LogicalFilter, LogicalProject, LogicalAggregate
- **Physical Plans** (`federated_query/plan/physical.py`): PhysicalScan, PhysicalHashJoin, PhysicalNestedLoopJoin, PhysicalBroadcastJoin
- **Expressions** (`federated_query/plan/expressions.py`): Column, Literal, BinaryOp, UnaryOp, AggregateCall

### Optimization
- **CostModel** (`federated_query/optimizer/cost.py`): Estimates query execution cost
- **Decorrelator** (`federated_query/optimizer/decorrelation.py`): Transforms correlated subqueries
- **Statistics** (`federated_query/optimizer/statistics.py`): Collects and uses table statistics

## Installation and Setup

### Install Dependencies
```bash
pip install -r requirements.txt
pip install -e .
```

### Install Development Tools
```bash
pip install pytest pytest-cov black mypy
```

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Tests with Coverage
```bash
pytest --cov=federated_query --cov-report=term-missing
```

### Run Specific Test File
```bash
pytest tests/test_parser.py -v
```

### Run Tests with PostgreSQL
The tests automatically use the PostgreSQL service when available:
```bash
# Using docker-compose
docker-compose up -d
pytest tests/test_datasources.py
```

## Code Formatting and Type Checking

### Format Code with Black
```bash
black federated_query tests
```

### Type Check with mypy
```bash
mypy federated_query
```

## Architecture Flow

```
SQL Query String
   ↓
Parser.parse() → sqlglot AST
   ↓
Parser.ast_to_logical_plan() → LogicalPlanNode
   ↓
Binder.bind() → Bound LogicalPlanNode (resolved references)
   ↓
RuleBasedOptimizer.optimize() → Optimized LogicalPlanNode
   ↓
PhysicalPlanner.plan() → PhysicalPlanNode (with cost estimation)
   ↓
Executor.execute() → Iterator[pa.RecordBatch] (Arrow format)
```

## Coding Standards and Rules

When working on this codebase, **strictly adhere** to the following rules:

### 1. Exception Handling

**NEVER catch bare exceptions.** We want to see exceptions at higher levels. Don't catch exceptions unnecessarily.

❌ **WRONG:**
```python
def parse(self, sql: str) -> exp.Expression:
    try:
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        return parsed
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {e}")
```

This is wrong because it adds complexity and hides the correct cause of the parse error.

✅ **CORRECT:**
```python
def parse(self, sql: str) -> exp.Expression:
    parsed = sqlglot.parse_one(sql, dialect=self.dialect)
    return parsed
```

**Only catch specific exceptions when you have a legitimate reason to handle them differently.** Never catch bare exceptions unless explicitly told or there is no other way.

### 2. List Comprehensions

**NEVER use list comprehensions.** Always use explicit loops for clarity and debuggability.

❌ **WRONG:**
```python
columns = [desc[0] for desc in cursor.description]
```

✅ **CORRECT:**
```python
columns = []
for desc in cursor.description:
    columns.append(desc[0])
```

### 3. Code Readability

**This code is written for humans.** Use proper naming and self-explanatory lines.

- Use descriptive variable names
- Avoid single-letter variables except for common conventions (i, j for loops)
- Write code that reads like prose
- Prefer clarity over cleverness

### 4. Cyclomatic Complexity

**No function/method can be over cyclomatic complexity of 4.**

Cyclomatic complexity measures the number of independent paths through code:
- Each if/elif adds 1
- Each loop (for/while) adds 1
- Each logical operator (and/or) adds 1
- Each except clause adds 1

If complexity exceeds 4, break the function into smaller helper functions.

### 5. Function Length

**No function/method can be longer than 20 lines.**

This does not include:
- Docstrings
- Blank lines
- Comments

If a function exceeds 20 lines of actual code, refactor it into smaller functions.

### 6. PEP-8 Compliance

**Follow PEP-8 rules as much as possible.**

- Use 4 spaces for indentation
- Maximum line length: 88 characters (Black formatter default)
- Two blank lines between top-level functions/classes
- One blank line between methods
- Import order: standard library, third-party, local
- Use snake_case for functions and variables
- Use PascalCase for classes

## Development Workflow

1. **Before making changes:** Run tests to ensure baseline works
2. **Make changes:** Follow the coding standards above
3. **Format code:** Run `black` to format
4. **Run tests:** Ensure all tests pass
5. **Check types:** Run `mypy` if touching type-heavy code
6. **Commit:** Write clear, descriptive commit messages

## Current Development Status

This project is in **Phase 0** (completed) with basic infrastructure:
- ✅ Project structure and skeleton
- ✅ Core abstractions (plans, expressions, data sources)
- ✅ Configuration system
- ✅ Basic catalog structure
- ✅ Test infrastructure
- ✅ Data source connectors (PostgreSQL, DuckDB)

See `tasks.md` for the full roadmap.

## Testing Strategy

- **Unit tests:** Test individual classes in isolation
- **Integration tests:** Test interactions between components
- **Database tests:** Use real PostgreSQL and DuckDB instances
- **Coverage target:** Aim for >80% code coverage

## Resources

- **Architecture details:** See `plan.md`
- **Implementation roadmap:** See `tasks.md`
- **Example config:** See `config/example_config.yaml`
- **README:** See `README.md` for user-facing documentation

---

**Remember:** Code quality and maintainability are paramount. Always prioritize readable, debuggable, and testable code over clever or compact solutions.
