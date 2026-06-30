"""The one DataType -> Arrow authority and the connector type contract.

A connector declares native-type -> DataType; the engine guarantees DataType ->
Arrow here. A DataType with no rendering raises (never a silent wrong type), and
the catalog enforces the contract at load so a drifting connector fails loudly.
"""

import pyarrow as pa
import pytest

from federated_query.plan.arrow_types import arrow_type_for, is_renderable
from federated_query.plan.expressions import DataType
from federated_query.catalog import Catalog


def test_arrow_type_for_renderable_types():
    """Each renderable DataType maps to its Arrow type."""
    assert arrow_type_for(DataType.INTEGER) == pa.int64()
    assert arrow_type_for(DataType.BIGINT) == pa.int64()
    assert arrow_type_for(DataType.DOUBLE) == pa.float64()
    assert arrow_type_for(DataType.VARCHAR) == pa.string()
    assert arrow_type_for(DataType.BOOLEAN) == pa.bool_()
    assert arrow_type_for(DataType.DATE) == pa.date32()
    assert arrow_type_for(DataType.TIMESTAMP) == pa.timestamp("us")


def test_arrow_type_for_raises_on_unrenderable():
    """A DataType with no Arrow rendering raises rather than defaulting."""
    with pytest.raises(ValueError):
        arrow_type_for(DataType.INTERVAL)
    with pytest.raises(ValueError):
        arrow_type_for(DataType.NULL)


def test_is_renderable_matches_the_authority():
    """is_renderable agrees with arrow_type_for on which types render."""
    assert is_renderable(DataType.INTEGER) is True
    assert is_renderable(DataType.INTERVAL) is False


def test_catalog_requires_renderable_datatype():
    """The catalog rejects a column whose mapped DataType has no Arrow rendering."""
    catalog = Catalog()
    with pytest.raises(ValueError) as exc:
        catalog._require_renderable("weird_col", DataType.INTERVAL)
    assert "weird_col" in str(exc.value)


def test_catalog_accepts_renderable_datatype():
    """A renderable column passes the contract check silently."""
    catalog = Catalog()
    catalog._require_renderable("ok_col", DataType.VARCHAR)
