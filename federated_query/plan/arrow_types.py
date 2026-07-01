"""The one engine DataType -> Arrow type mapping.

A single authority shared by the executor (local CAST) and the catalog
(load-time renderability check), so a column's declared DataType has exactly one
Arrow rendering. A connector declares native-type -> DataType (its
map_native_type); the engine guarantees DataType -> Arrow here. A DataType with
no rendering fails loudly rather than defaulting to a wrong type or crashing at
an arbitrary later point.
"""

import pyarrow as pa

from .expressions import DataType

# Engine DataType -> the Arrow type it renders as. The renderable DataTypes are
# exactly those a connector's map_native_type is allowed to produce; types that
# never reach a column (INTERVAL, NULL) are intentionally absent.
_DATATYPE_TO_ARROW = {
    DataType.INTEGER: pa.int64(),
    DataType.BIGINT: pa.int64(),
    DataType.FLOAT: pa.float32(),
    DataType.DOUBLE: pa.float64(),
    DataType.DECIMAL: pa.float64(),
    DataType.VARCHAR: pa.string(),
    DataType.TEXT: pa.string(),
    DataType.BOOLEAN: pa.bool_(),
    DataType.DATE: pa.date32(),
    DataType.TIMESTAMP: pa.timestamp("us"),
}


def arrow_type_for(data_type: DataType) -> pa.DataType:
    """The Arrow type for an engine DataType; raise on one with no mapping.

    An unmapped DataType is a gap to fill explicitly here, never a silent default
    to string (which would mistype a column and ship a wrong answer).
    """
    if data_type not in _DATATYPE_TO_ARROW:
        raise ValueError(f"No Arrow type for DataType {data_type.value}")
    return _DATATYPE_TO_ARROW[data_type]


def is_renderable(data_type: DataType) -> bool:
    """Whether a DataType has an Arrow rendering - the connector type contract."""
    return data_type in _DATATYPE_TO_ARROW
