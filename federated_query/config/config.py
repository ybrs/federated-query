"""Configuration management for federated query engine."""

from typing import Dict, Any, List, Optional
import yaml
from pathlib import Path

from pydantic import Field

from ..model import StateModel


class DataSourceConfig(StateModel):
    """Configuration for a single data source."""

    name: str
    type: str  # "postgresql", "duckdb", etc.
    config: Dict[str, Any]
    capabilities: List[str] = Field(default_factory=list)


class OptimizerConfig(StateModel):
    """Configuration for query optimizer."""

    enable_predicate_pushdown: bool = True
    enable_projection_pushdown: bool = True
    enable_join_reordering: bool = True
    enable_decorrelation: bool = True
    max_join_reorder_size: int = 10  # Use DP for <= this many tables


class ExecutorConfig(StateModel):
    """Configuration for query executor."""

    max_memory_mb: int = 1024
    batch_size: int = 10000
    max_threads: int = 4
    enable_parallel_fetch: bool = True
    # Local "merge engine" (in-memory DuckDB coordinator) settings. The engine
    # merges the Arrow streams returned by remote sources; it buffers only what
    # the algorithm requires and spills to ``merge_engine_temp_directory`` once
    # it exceeds ``merge_engine_memory_limit``. The temp directory is left to
    # DuckDB's default when None.
    merge_engine_memory_limit: str = "1GB"
    merge_engine_temp_directory: Optional[str] = None


class CostConfig(StateModel):
    """Configuration for cost model."""

    cpu_tuple_cost: float = 0.01
    io_page_cost: float = 1.0
    network_byte_cost: float = 0.0001
    network_rtt_ms: float = 10.0


class Config(StateModel):
    """Main configuration class."""

    datasources: Dict[str, DataSourceConfig] = Field(default_factory=dict)
    optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    cost: CostConfig = Field(default_factory=CostConfig)


def load_config(config_path: str) -> Config:
    """Load configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Parsed configuration

    Example YAML format:
        datasources:
          postgres_prod:
            type: postgresql
            host: localhost
            port: 5432
            database: mydb
            user: user
            password: pass
            schemas: [public]
            capabilities: [aggregations, joins, window_functions]

          local_duckdb:
            type: duckdb
            path: /data/local.duckdb
            read_only: true
            capabilities: [aggregations, joins]

        optimizer:
          enable_predicate_pushdown: true
          enable_join_reordering: true
          max_join_reorder_size: 10

        executor:
          max_memory_mb: 2048
          batch_size: 10000
          max_threads: 8

        cost:
          cpu_tuple_cost: 0.01
          network_byte_cost: 0.0001
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    # Parse data sources
    datasources = {}
    for name, ds_config in data.get("datasources", {}).items():
        ds_type = ds_config.pop("type")
        capabilities = ds_config.pop("capabilities", [])
        datasources[name] = DataSourceConfig(
            name=name, type=ds_type, config=ds_config, capabilities=capabilities
        )

    # Parse optimizer config
    optimizer_data = data.get("optimizer", {})
    optimizer = OptimizerConfig(**optimizer_data)

    # Parse executor config
    executor_data = data.get("executor", {})
    executor = ExecutorConfig(**executor_data)

    # Parse cost config
    cost_data = data.get("cost", {})
    cost = CostConfig(**cost_data)

    return Config(
        datasources=datasources, optimizer=optimizer, executor=executor, cost=cost
    )
