"""Lint: the engine uses no Python dataclasses anywhere.

Final design decision (CLAUDE.md / AGENTS.md): every state-carrying type is a
plain (mutable) Pydantic model deriving from ``federated_query.model.StateModel``
- never a ``@dataclass``. This is the root-cause guard against the silent
field-drop bug class (a reconstruction that forgets a field takes the default);
``model_copy(update=...)`` copies every field, so a field can never be dropped.

This test parses the source with ``ast`` (not a text grep) so that prose
mentioning "dataclass" in a docstring or comment never trips it - only real
``@dataclass`` decorators and ``dataclasses`` imports fail.
"""

import ast
import pathlib

import federated_query

_PACKAGE_ROOT = pathlib.Path(federated_query.__file__).parent
_REPO_ROOT = _PACKAGE_ROOT.parent
_TESTS_ROOT = _REPO_ROOT / "tests"


def _python_files():
    """Every .py file in the package and the test suite.

    The tests are covered too: a ``@dataclass`` in a test fixture (e.g. a
    conftest) is the same silent field-drop hazard, which a package-only scan
    would miss.
    """
    files = list(_PACKAGE_ROOT.rglob("*.py"))
    files.extend(_TESTS_ROOT.rglob("*.py"))
    return sorted(files)


def _offenders(tree: ast.AST):
    """Yield human-readable reasons a module uses dataclasses, if any."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "dataclasses" or alias.name.startswith("dataclasses."):
                    yield f"line {node.lineno}: import {alias.name}"
        elif isinstance(node, ast.ImportFrom):
            if node.module == "dataclasses":
                yield f"line {node.lineno}: from dataclasses import ..."
        elif isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            for decorator in node.decorator_list:
                if _decorator_name(decorator) == "dataclass":
                    yield f"line {decorator.lineno}: @dataclass on {node.name}"


def _decorator_name(decorator: ast.expr):
    """Resolve a decorator to its base name (handles @dataclass(...) calls)."""
    if isinstance(decorator, ast.Call):
        decorator = decorator.func
    if isinstance(decorator, ast.Attribute):
        return decorator.attr
    if isinstance(decorator, ast.Name):
        return decorator.id
    return None


def test_no_dataclasses_in_package():
    """No module in the package or tests imports dataclasses or uses @dataclass."""
    failures = {}
    for path in _python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        reasons = list(_offenders(tree))
        if reasons:
            failures[str(path.relative_to(_REPO_ROOT))] = reasons
    assert not failures, "dataclasses found (use StateModel instead):\n" + "\n".join(
        f"  {f}: {', '.join(rs)}" for f, rs in failures.items()
    )
