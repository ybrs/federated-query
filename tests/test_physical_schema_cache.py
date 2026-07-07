"""The physical-node derived-value cache (schema / column_aliases).

Both derivations recurse the whole subtree and every parent re-derives its
children's results - exponential in tree depth (TPC-DS q59 spent 4.6s of a
4.6s run re-deriving schemas during IR build). These tests pin the caching
contract: computed once per node, per-instance, and a model_copy starts with
an EMPTY cache because its fields may differ.
"""

from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from federated_query.plan.physical import PhysicalPlanNode

_CALLS = {"schema": 0, "aliases": 0}


class _Probe(PhysicalPlanNode):
    """A minimal node counting how often its derivations actually run."""

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def schema(self) -> pa.Schema:
        _CALLS["schema"] += 1
        return pa.schema([("a", pa.int64())])

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        _CALLS["aliases"] += 1
        return {(None, "a"): "a"}

    def estimated_cost(self) -> float:
        return 0.0


def test_schema_computed_once_per_node():
    """Repeated schema()/column_aliases() calls compute exactly once."""
    _CALLS["schema"] = 0
    _CALLS["aliases"] = 0
    probe = _Probe()
    assert probe.schema() is probe.schema()
    assert probe.column_aliases() is probe.column_aliases()
    assert _CALLS["schema"] == 1
    assert _CALLS["aliases"] == 1


def test_cache_is_per_instance():
    """A second node computes its own derivations."""
    _CALLS["schema"] = 0
    first = _Probe()
    second = _Probe()
    first.schema()
    second.schema()
    assert _CALLS["schema"] == 2


def test_model_copy_starts_with_empty_cache():
    """A copy re-derives: its fields may differ from the original's."""
    _CALLS["schema"] = 0
    probe = _Probe()
    probe.schema()
    copied = probe.model_copy()
    copied.schema()
    assert _CALLS["schema"] == 2
