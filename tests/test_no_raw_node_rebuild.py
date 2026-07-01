"""Lint: transform code must not rebuild a node with a raw constructor.

Re-listing an existing node's fields into ``Node(...)`` silently drops any field
the call omits (the dataclass-era bug class that survives wherever transform code
hand-rolls a constructor instead of ``node.model_copy(update=...)``). model_copy
copies every field, so an omitted/future field can never be dropped.

This guards the conventional rebuild shape: ``T(..., field=VAR.field, ...)`` where
``VAR`` is named after ``T`` (e.g. ``Join(..., condition=join.condition)``). A
genuinely new node does not copy a same-type source node's fields by identical
name, so cross-type lowering (logical Scan -> PhysicalScan) and fresh synthesis
are not flagged. Rebuilds that legitimately must use a constructor should instead
be expressed with model_copy; there is no escape hatch because a same-type
re-list is never the right tool in transform code.
"""

import ast
import pathlib

import federated_query
from federated_query.model import StateModel
from federated_query.plan import logical, physical, expressions

_PACKAGE_ROOT = pathlib.Path(federated_query.__file__).parent

# Plan / expression node classes whose raw reconstruction drops fields.
_NODE_CLASSES = {
    name
    for module in (logical, physical, expressions)
    for name, obj in vars(module).items()
    if isinstance(obj, type) and issubclass(obj, StateModel) and obj is not StateModel
}

# Modules that rewrite an existing plan, where a same-type constructor is a
# drop-prone rebuild rather than fresh construction.
_TRANSFORM_MODULES = [
    "parser/binder.py",
    "optimizer/rules.py",
    "optimizer/decorrelation.py",
    "optimizer/single_source_pushdown.py",
    "optimizer/physical_planner.py",
]


def _rebuild_source(call: ast.Call):
    """Return the source variable name if this call rebuilds a same-type node.

    True when a keyword argument copies a field unchanged from a variable named
    after the constructed type, e.g. ``Join(..., join_type=join.join_type)``.
    """
    type_name = call.func.id
    for keyword in call.keywords:
        value = keyword.value
        if (
            isinstance(value, ast.Attribute)
            and isinstance(value.value, ast.Name)
            and keyword.arg == value.attr
            and value.value.id.lower() == type_name.lower()
        ):
            return value.value.id
    return None


def _violations(source: str, tree: ast.AST):
    """Yield (line, type, source_var) for each same-type rebuild in a module."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name):
            continue
        if node.func.id not in _NODE_CLASSES:
            continue
        source_var = _rebuild_source(node)
        if source_var is not None:
            yield node.lineno, node.func.id, source_var


def test_no_raw_node_rebuild_in_transforms():
    """Transform modules rebuild nodes via model_copy, never a raw constructor."""
    failures = {}
    for relative in _TRANSFORM_MODULES:
        path = _PACKAGE_ROOT / relative
        tree = ast.parse(path.read_text(), filename=str(path))
        offenders = list(_violations(path.read_text(), tree))
        if offenders:
            failures[relative] = offenders
    assert not failures, (
        "raw same-type node rebuilds found (use node.model_copy(update=...)):\n"
        + "\n".join(
            f"  {mod}:\n"
            + "\n".join(f"    line {ln}: {t}(... {v}.x ...)" for ln, t, v in offs)
            for mod, offs in failures.items()
        )
    )
