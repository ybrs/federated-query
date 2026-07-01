"""Lint: every .create(...) call site must be defended by >= 2 comment lines.

Project decision: constructing a state object is a deliberate act, so each
``ClassName.create(...)`` call in ``federated_query/`` must carry at least two
comment lines directly above the statement, explaining what the node is and why
it is built there. This forces a justification at every construction site and
makes a thoughtless build visible in review.

The comment block must be contiguous and immediately above the statement (no
blank line between the comments and the statement). Only calls whose receiver is
a StateModel subclass are checked, so an unrelated ``.create`` on some other
object is not caught. Tests are intentionally out of scope for now.
"""

import ast
import importlib
import pathlib
import pkgutil

import federated_query
from federated_query.model import StateModel

_PACKAGE_ROOT = pathlib.Path(federated_query.__file__).parent


def _state_class_names():
    """Return the set of every StateModel subclass name in the engine."""
    for module in pkgutil.walk_packages([str(_PACKAGE_ROOT)], prefix="federated_query."):
        importlib.import_module(module.name)
    names = set()
    pending = list(StateModel.__subclasses__())
    while pending:
        cls = pending.pop()
        names.add(cls.__name__)
        pending.extend(cls.__subclasses__())
    return names


def _statement_lines(tree: ast.AST):
    """Map each AST node to the line of its nearest enclosing statement.

    The comment-defense rule anchors to the statement that introduces a build,
    so a multi-line call is judged by the line its statement starts on.
    """
    line_of = {}
    for statement in ast.walk(tree):
        if not isinstance(statement, ast.stmt):
            continue
        for child in ast.walk(statement):
            line_of[child] = statement.lineno
    return line_of


def _create_calls(tree: ast.AST, state_names):
    """Yield each ``StateClass.create(...)`` call node in the module."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        if not isinstance(callee, ast.Attribute) or callee.attr != "create":
            continue
        if isinstance(callee.value, ast.Name) and callee.value.id in state_names:
            yield node


def _comment_lines_above(lines, statement_line):
    """Count contiguous comment lines directly above a statement (1-indexed)."""
    count = 0
    index = statement_line - 2
    while index >= 0 and lines[index].strip().startswith("#"):
        count += 1
        index -= 1
    return count


def test_create_calls_are_comment_defended():
    """Each .create(...) call in the engine has >= 2 comment lines above it."""
    state_names = _state_class_names()
    failures = {}
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        source = path.read_text()
        lines = source.splitlines()
        tree = ast.parse(source, filename=str(path))
        line_of = _statement_lines(tree)
        offenders = []
        for call in _create_calls(tree, state_names):
            statement_line = line_of.get(call, call.lineno)
            if _comment_lines_above(lines, statement_line) < 2:
                offenders.append(statement_line)
        if offenders:
            failures[str(path.relative_to(_PACKAGE_ROOT))] = sorted(set(offenders))
    assert not failures, (
        "undefended .create(...) call sites (need >= 2 comment lines directly "
        "above the statement):\n"
        + "\n".join(
            f"  {module}: lines {offenders}" for module, offenders in failures.items()
        )
    )
