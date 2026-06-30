"""Lint: a node rebuild must not silently drop a defaulted field.

CLAUDE.md forbids reconstructing a state node by re-listing its fields into the
raw constructor: an omitted field takes its default, a silent field-drop (the
exact bug that lost a FunctionCall's WITHIN GROUP key and a Join's NATURAL flag).
``model_copy(update=...)`` copies every field, so it can never drop one - that is
the one allowed way to rebuild a node of the same type.

This test parses the source with ``ast`` and flags the dangerous signature:
constructing a node class while re-listing one of its fields off an instance
(``Cls(field=x.field, ...)``) AND omitting at least one field that has a default.
A bare constructor is fine for FRESH construction (the parser building nodes from
sqlglot) and for CROSS-TYPE translation (logical -> physical), which genuinely
cannot use model_copy; those few sites are allowlisted below with their reason.
"""

import ast
import pathlib

import federated_query
import federated_query.plan.logical as _logical
import federated_query.plan.expressions as _expressions
import federated_query.plan.physical as _physical
import federated_query.catalog.schema as _schema
import federated_query.datasources.base as _base

_PACKAGE_ROOT = pathlib.Path(federated_query.__file__).parent

# (relative_path, class_name) sites that legitimately re-list because the source
# is a DIFFERENT type than the constructed node, so model_copy does not apply.
_ALLOWED_CROSS_TYPE = {
    # logical plan node -> physical plan node (the omitted fields are
    # physical-only and set later during planning).
    ("optimizer/physical_planner.py", "PhysicalScan"),
    ("optimizer/physical_planner.py", "PhysicalRemoteJoin"),
    ("optimizer/physical_planner.py", "PhysicalRemoteSetOp"),
    # sqlglot AST table -> engine CTERef (output_names filled from the catalog).
    ("parser/parser.py", "CTERef"),
}


def _node_fields():
    """Map each node class name to {field_name: has_default}."""
    classes = {}
    for module in (_logical, _expressions, _physical, _schema, _base):
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and hasattr(obj, "model_fields"):
                classes[name] = {
                    field: not info.is_required()
                    for field, info in obj.model_fields.items()
                }
    return classes


def _relists_a_field(call: ast.Call) -> bool:
    """Whether a call passes ``field=x.field`` (re-listing a field by name)."""
    for keyword in call.keywords:
        value = keyword.value
        if keyword.arg and isinstance(value, ast.Attribute) and value.attr == keyword.arg:
            return True
    return False


def _drops_defaulted_field(call: ast.Call, fields) -> bool:
    """Whether the call omits at least one field that has a default."""
    passed = set()
    for keyword in call.keywords:
        if keyword.arg:
            passed.add(keyword.arg)
    omitted = set(fields) - passed
    if not (passed < set(fields)):
        return False
    return any(fields[name] for name in omitted)


def test_no_node_rebuild_drops_a_defaulted_field():
    """No same-type node rebuild silently drops a defaulted field."""
    node_fields = _node_fields()
    offenders = []
    for path in _PACKAGE_ROOT.rglob("*.py"):
        rel = str(path.relative_to(_PACKAGE_ROOT))
        tree = ast.parse(path.read_text(), str(path))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
                continue
            cls = node.func.id
            if cls not in node_fields:
                continue
            if not _relists_a_field(node):
                continue
            if not _drops_defaulted_field(node, node_fields[cls]):
                continue
            if (rel, cls) in _ALLOWED_CROSS_TYPE:
                continue
            offenders.append(f"{rel}:{node.lineno} {cls}(...) - use model_copy")
    assert not offenders, "Node rebuilds that drop a defaulted field:\n" + "\n".join(
        offenders
    )
