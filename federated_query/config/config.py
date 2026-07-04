"""Configuration management for federated query engine."""

from typing import Dict, Any, List
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

    @classmethod
    def create(
        cls,
        *,
        name: str,
        type: str,
        config: Dict[str, Any],
        capabilities: List[str],
    ) -> "DataSourceConfig":
        """Sanctioned fresh-construction path for DataSourceConfig.
        Names every field so none is dropped; capabilities is explicit (the
        default_factory cannot be a parameter default) - pass [] for none."""
        return cls(name=name, type=type, config=config, capabilities=capabilities)


class OptimizerConfig(StateModel):
    """Configuration for query optimizer."""

    enable_predicate_pushdown: bool = True
    enable_projection_pushdown: bool = True
    enable_join_reordering: bool = True
    enable_decorrelation: bool = True
    max_join_reorder_size: int = 10  # Use DP for <= this many tables

    @classmethod
    def create(
        cls,
        *,
        enable_predicate_pushdown: bool = True,
        enable_projection_pushdown: bool = True,
        enable_join_reordering: bool = True,
        enable_decorrelation: bool = True,
        max_join_reorder_size: int = 10,
    ) -> "OptimizerConfig":
        """Sanctioned fresh-construction path for OptimizerConfig.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            enable_predicate_pushdown=enable_predicate_pushdown,
            enable_projection_pushdown=enable_projection_pushdown,
            enable_join_reordering=enable_join_reordering,
            enable_decorrelation=enable_decorrelation,
            max_join_reorder_size=max_join_reorder_size,
        )


class ExecutorConfig(StateModel):
    """Configuration for query executor."""

    max_memory_mb: int = 1024
    batch_size: int = 10000
    max_threads: int = 4
    enable_parallel_fetch: bool = True

    @classmethod
    def create(
        cls,
        *,
        max_memory_mb: int = 1024,
        batch_size: int = 10000,
        max_threads: int = 4,
        enable_parallel_fetch: bool = True,
    ) -> "ExecutorConfig":
        """Sanctioned fresh-construction path for ExecutorConfig.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            max_memory_mb=max_memory_mb,
            batch_size=batch_size,
            max_threads=max_threads,
            enable_parallel_fetch=enable_parallel_fetch,
        )


class CostConfig(StateModel):
    """Configuration for cost model."""

    cpu_tuple_cost: float = 0.01
    io_page_cost: float = 1.0
    network_byte_cost: float = 0.0001
    network_rtt_ms: float = 10.0

    @classmethod
    def create(
        cls,
        *,
        cpu_tuple_cost: float = 0.01,
        io_page_cost: float = 1.0,
        network_byte_cost: float = 0.0001,
        network_rtt_ms: float = 10.0,
    ) -> "CostConfig":
        """Sanctioned fresh-construction path for CostConfig.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            cpu_tuple_cost=cpu_tuple_cost,
            io_page_cost=io_page_cost,
            network_byte_cost=network_byte_cost,
            network_rtt_ms=network_rtt_ms,
        )


class Config(StateModel):
    """Main configuration class."""

    datasources: Dict[str, DataSourceConfig] = Field(default_factory=dict)
    optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    cost: CostConfig = Field(default_factory=CostConfig)

    @classmethod
    def create(
        cls,
        *,
        datasources: Dict[str, DataSourceConfig],
        optimizer: OptimizerConfig,
        executor: ExecutorConfig,
        cost: CostConfig,
    ) -> "Config":
        """Sanctioned fresh-construction path for Config.
        Every section is explicit (the default_factory sub-configs cannot be
        parameter defaults); build empty/defaulted sections at the call site."""
        return cls(
            datasources=datasources,
            optimizer=optimizer,
            executor=executor,
            cost=cost,
        )


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

    # Reject unknown top-level sections so a typo (e.g. "optimizr:") fails loud
    # instead of being silently ignored and falling back to defaults. The nested
    # models already forbid unknown keys; this covers the section names too.
    known_sections = {"datasources", "optimizer", "executor", "cost"}
    unknown_sections = set(data) - known_sections
    if unknown_sections:
        raise ValueError(
            f"Unknown config section(s): {', '.join(sorted(unknown_sections))}"
        )

    # Parse data sources
    datasources = {}
    for name, ds_config in data.get("datasources", {}).items():
        ds_type = ds_config.pop("type")
        capabilities = ds_config.pop("capabilities", [])
        # One data source entry built from its YAML block; type and capabilities
        # were popped out above so config holds only the connector settings.
        datasources[name] = DataSourceConfig.create(
            name=name, type=ds_type, config=ds_config, capabilities=capabilities
        )

    # Parse optimizer config
    optimizer_data = data.get("optimizer", {})
    # Optimizer settings from the YAML section, or defaults when it is absent.
    # Keyword-expanded so an unknown key raises rather than being dropped.
    optimizer = OptimizerConfig.create(**optimizer_data)

    # Parse executor config
    executor_data = data.get("executor", {})
    # Executor settings from the YAML section, defaulting when the section is
    # missing; keys are validated by the model on construction.
    executor = ExecutorConfig.create(**executor_data)

    # Parse cost config
    cost_data = data.get("cost", {})
    # Cost-model settings from the YAML section, defaulting when absent so the
    # cost estimator always has a concrete configuration.
    cost = CostConfig.create(**cost_data)

    # The fully assembled engine config, wiring the parsed data sources together
    # with the optimizer, executor, and cost sub-configs built above.
    return Config.create(
        datasources=datasources, optimizer=optimizer, executor=executor, cost=cost
    )
