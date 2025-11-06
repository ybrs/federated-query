"""Tests for configuration loading."""

import pytest
from pathlib import Path
from federated_query.config import load_config


def test_load_example_config():
    """Test loading the example configuration."""
    config_path = Path(__file__).parent.parent / "config" / "example_config.yaml"

    if not config_path.exists():
        pytest.skip("Example config not found")

    config = load_config(str(config_path))

    # Verify data sources
    assert "postgres_prod" in config.datasources
    assert "local_duckdb" in config.datasources

    # Verify optimizer config
    assert config.optimizer.enable_predicate_pushdown is True
    assert config.optimizer.max_join_reorder_size == 10

    # Verify executor config
    assert config.executor.max_memory_mb == 2048
    assert config.executor.batch_size == 10000
