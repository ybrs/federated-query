"""StateModel keeps construction and copy-with-change LOUD.

The point of dropping dataclasses was to stop silent field bugs. Pydantic's
defaults would reintroduce two of them: ``extra="ignore"`` silently drops an
unknown construction kwarg, and ``model_copy(update=...)`` does not validate its
keys (a typo'd key sets a junk attribute and drops the intended change). The
``StateModel`` base closes both; these tests pin that they stay closed.
"""

from typing import Optional

import pytest
from pydantic import ValidationError

from federated_query.model import StateModel


class _Sample(StateModel):
    a: int
    b: Optional[str] = None


def test_unknown_construction_kwarg_raises():
    """A stray/renamed kwarg is an error, never silently dropped."""
    with pytest.raises(ValidationError):
        _Sample(a=1, c=2)


def test_missing_required_field_raises():
    """A field with no default is required."""
    with pytest.raises(ValidationError):
        _Sample(b="x")


def test_model_copy_unknown_key_raises():
    """A mistyped update key is loud, not a silently-set junk attribute."""
    s = _Sample(a=1, b="x")
    with pytest.raises(ValueError) as exc:
        s.model_copy(update={"bb": "y"})
    assert "unknown field" in str(exc.value)
    assert not hasattr(s.model_copy(update={"b": "y"}), "bb")


def test_model_copy_valid_key_preserves_rest():
    """A real update overrides only the named field and copies the rest."""
    s = _Sample(a=1, b="x")
    copy = s.model_copy(update={"b": "y"})
    assert copy.a == 1 and copy.b == "y"
    assert s.b == "x"  # original untouched
