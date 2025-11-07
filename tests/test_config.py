"""Tests for configuration loading."""

import pytest
import tempfile
from pathlib import Path
from federated_query.config import load_config, Config, DataSourceConfig


def test_load_example_config():
    """Test loading the example configuration."""
    config_path = Path(__file__).parent.parent / "config" / "example_config.yaml"

    if not config_path.exists():
        pytest.skip("Example config not found")

    config = load_config(str(config_path))

    # Verify data sources
    assert "postgres_prod" in config.datasources
    assert "local_duckdb" in config.datasources

    # Verify PostgreSQL config
    pg_config = config.datasources["postgres_prod"]
    assert pg_config.type == "postgresql"
    assert pg_config.config["host"] == "localhost"
    assert pg_config.config["port"] == 5432
    assert pg_config.config["database"] == "analytics"

    # Verify DuckDB config
    duck_config = config.datasources["local_duckdb"]
    assert duck_config.type == "duckdb"
    assert "path" in duck_config.config

    # Verify optimizer config
    assert config.optimizer.enable_predicate_pushdown is True
    assert config.optimizer.enable_projection_pushdown is True
    assert config.optimizer.enable_join_reordering is True
    assert config.optimizer.max_join_reorder_size == 10

    # Verify executor config
    assert config.executor.max_memory_mb == 2048
    assert config.executor.batch_size == 10000
    assert config.executor.max_threads == 8

    # Verify cost config
    assert config.cost.cpu_tuple_cost == 0.01
    assert config.cost.io_page_cost == 1.0


def test_load_minimal_config():
    """Test loading minimal configuration with defaults."""
    # Create a minimal config file
    minimal_yaml = """
datasources:
  test_pg:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(minimal_yaml)
        config_path = f.name

    try:
        config = load_config(config_path)

        # Verify data source exists
        assert "test_pg" in config.datasources
        assert config.datasources["test_pg"].type == "postgresql"

        # Verify defaults are applied
        assert config.optimizer.enable_predicate_pushdown is True
        assert config.executor.max_memory_mb == 1024
        assert config.cost.cpu_tuple_cost == 0.01
    finally:
        Path(config_path).unlink()


def test_missing_config_file():
    """Test error handling for missing config file."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent_config.yaml")


def test_config_with_capabilities():
    """Test configuration with data source capabilities."""
    config_yaml = """
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test
    capabilities:
      - aggregations
      - joins
      - window_functions
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_yaml)
        config_path = f.name

    try:
        config = load_config(config_path)
        ds_config = config.datasources["test_db"]

        assert "aggregations" in ds_config.capabilities
        assert "joins" in ds_config.capabilities
        assert "window_functions" in ds_config.capabilities
    finally:
        Path(config_path).unlink()


def test_multiple_datasources():
    """Test configuration with multiple data sources."""
    config_yaml = """
datasources:
  pg1:
    type: postgresql
    host: host1
    database: db1
    user: user1
    password: pass1

  pg2:
    type: postgresql
    host: host2
    database: db2
    user: user2
    password: pass2

  duck1:
    type: duckdb
    path: /path/to/db.duckdb
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_yaml)
        config_path = f.name

    try:
        config = load_config(config_path)

        assert len(config.datasources) == 3
        assert "pg1" in config.datasources
        assert "pg2" in config.datasources
        assert "duck1" in config.datasources

        assert config.datasources["pg1"].type == "postgresql"
        assert config.datasources["pg2"].type == "postgresql"
        assert config.datasources["duck1"].type == "duckdb"
    finally:
        Path(config_path).unlink()


def test_optimizer_config_override():
    """Test overriding optimizer configuration."""
    config_yaml = """
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test

optimizer:
  enable_predicate_pushdown: false
  enable_join_reordering: false
  max_join_reorder_size: 20
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_yaml)
        config_path = f.name

    try:
        config = load_config(config_path)

        assert config.optimizer.enable_predicate_pushdown is False
        assert config.optimizer.enable_join_reordering is False
        assert config.optimizer.max_join_reorder_size == 20
    finally:
        Path(config_path).unlink()


def test_executor_config_override():
    """Test overriding executor configuration."""
    config_yaml = """
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test

executor:
  max_memory_mb: 4096
  batch_size: 50000
  max_threads: 16
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_yaml)
        config_path = f.name

    try:
        config = load_config(config_path)

        assert config.executor.max_memory_mb == 4096
        assert config.executor.batch_size == 50000
        assert config.executor.max_threads == 16
    finally:
        Path(config_path).unlink()
