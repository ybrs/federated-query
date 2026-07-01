"""Lint: engine code must construct every state object via .create(...).

A bare initializer -- ``Node(field=...)`` called directly on a state class --
is forbidden anywhere in ``federated_query/``. Construction goes through the
class's ``.create`` classmethod (fresh construction / cross-type lowering) or
through ``instance.model_copy(update=...)`` (deriving from an existing node).

Rationale (project decision): ``.create`` is a semantic marker, not just a code
guard. A diff that introduces a bare ``Node(...)`` is immediately visible as
off-protocol, and every construction site is forced through one named,
greppable, comment-defended path (see test_create_needs_comment). The single
sanctioned bare init is ``cls(...)`` INSIDE a ``.create`` body itself; that is a
``cls`` call, not a state-class-name call, so it is not flagged here.

The check is AST-based over the source (not a text grep) so a class name in a
string or comment never trips it. Tests are intentionally out of scope for now.
"""

import ast
import importlib
import pathlib
import pkgutil

import federated_query
from federated_query.model import StateModel

_PACKAGE_ROOT = pathlib.Path(federated_query.__file__).parent


def _state_class_names():
    """Return the set of every StateModel subclass name in the engine.

    Imports every submodule so the full subclass tree is realized, then walks
    ``StateModel.__subclasses__`` transitively. A name here is one whose bare
    construction the lint forbids.
    """
    for module in pkgutil.walk_packages([str(_PACKAGE_ROOT)], prefix="federated_query."):
        importlib.import_module(module.name)
    names = set()
    pending = list(StateModel.__subclasses__())
    while pending:
        cls = pending.pop()
        names.add(cls.__name__)
        pending.extend(cls.__subclasses__())
    return names


def _bare_inits(tree: ast.AST, state_names):
    """Yield (line, class_name) for each bare state-class construction in a module.

    A bare init is a call whose callee is a plain Name matching a state class
    (``Node(...)``). ``Node.create(...)`` is an Attribute call and ``cls(...)``
    is a non-class Name, so neither is reported.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name):
            continue
        if node.func.id in state_names:
            yield node.lineno, node.func.id


def test_no_bare_state_class_init_in_engine():
    """No module under federated_query/ constructs a state class with a bare init."""
    state_names = _state_class_names()
    failures = {}
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        offenders = list(_bare_inits(tree, state_names))
        if offenders:
            failures[str(path.relative_to(_PACKAGE_ROOT))] = offenders
    assert not failures, (
        "bare state-class initializers found (use ClassName.create(...) or "
        "instance.model_copy(update=...)):\n"
        + "\n".join(
            f"  {module}:\n"
            + "\n".join(f"    line {line}: {name}(...)" for line, name in offenders)
            for module, offenders in failures.items()
        )
    )
