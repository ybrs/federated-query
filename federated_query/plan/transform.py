"""Transitional copy-with-change helper used during the Pydantic migration.

``replace(node, field=value)`` returns a copy of ``node`` with the named fields
overridden and every other field preserved -- the drop-proof operation. For a
Pydantic model it is ``model_copy(update=...)``; for the layers not yet migrated
off dataclasses it falls back to ``dataclasses.replace``. Once every layer is a
Pydantic model the dataclass branch is dead and call sites move to
``model_copy`` directly.
"""

import dataclasses

from pydantic import BaseModel


def replace(obj, **changes):
    """Copy ``obj`` overriding the named fields; preserves all the rest."""
    if isinstance(obj, BaseModel):
        return obj.model_copy(update=changes)
    return dataclasses.replace(obj, **changes)
