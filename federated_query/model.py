"""Project-wide Pydantic base for state-carrying types.

Every state-carrying type (plan/expression nodes, catalog, config, datasource
metadata, planner helpers) is a plain (mutable) Pydantic model - never a
dataclass (see CLAUDE.md). ``StateModel`` adds three things over a plain
``BaseModel``, all chosen to keep mistakes LOUD (the whole point of dropping
dataclasses was to stop silent field bugs):

- ``arbitrary_types_allowed`` so a model can hold non-model values (Expression,
  sqlglot, pyarrow, enums).
- ``extra="forbid"`` so an unknown construction kwarg raises instead of being
  silently dropped (Pydantic's default ``extra="ignore"`` would hide a renamed
  or mistyped field at the call site).
- a ``model_copy`` override that rejects unknown ``update`` keys, since plain
  ``model_copy(update=...)`` does NOT validate keys and would otherwise set a
  junk attribute silently.

Construction is keyword-only (Pydantic's default) - by design: keyword
construction names every field, so reordering a field declaration can never
silently rebind a call site to the wrong field.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict


class StateModel(BaseModel):
    """Base for every state-carrying type in the engine.

    A mutable Pydantic model: fields are declared as annotations (no default =
    required, enforced by Pydantic), construction is keyword-only, copies use
    ``model_copy(update=...)`` so a field can never be dropped, and equality is
    the structural comparison Pydantic derives over every field.
    ``arbitrary_types_allowed`` lets a model hold Expression, sqlglot, pyarrow
    and other non-model values; ``extra="forbid"`` makes a stray kwarg loud.
    """

    # arbitrary_types_allowed: models hold non-model values (Expression, sqlglot).
    # extra="forbid": an unknown construction kwarg is an error, never silently dropped.
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    def model_copy(
        self, *, update: Optional[Dict[str, Any]] = None, deep: bool = False
    ) -> "StateModel":
        """Copy with field overrides, rejecting unknown ``update`` keys.

        Plain ``BaseModel.model_copy`` does not validate ``update`` - a mistyped
        key would set a junk attribute and silently drop the intended change.
        Guarding here keeps copy-with-change as loud as construction.
        """
        if update:
            unknown = set(update) - set(type(self).model_fields)
            if unknown:
                raise ValueError(
                    f"{type(self).__name__}.model_copy() got unknown field(s) "
                    f"{sorted(unknown)}; valid fields are "
                    f"{sorted(type(self).model_fields)}"
                )
        return super().model_copy(update=update, deep=deep)
