"""flake8 plugin: state-object construction rules for the engine.

Registered as a flake8 local plugin (see the project .flake8). Two codes, both
checked only for source under ``federated_query/`` (never tests):

  FQ001  Bare init of a state class (``Node(field=...)`` called directly on the
         class) is forbidden. Construct via ``Node.create(...)`` or
         ``instance.model_copy(update=...)``. A bare init is tolerated ONLY when
         the two source lines directly above the statement are comments that
         justify it.

  FQ002  Every ``Node.create(...)`` call needs at least two comment lines
         directly above its statement (the comments that say what is built and
         why).

This is an AST plugin, not a text scan, for two reasons a regex cannot handle:
a nested call (``A.create(b=B.create(...))``) is attributed to the ONE enclosing
statement, so the statement's comments defend the whole tree instead of each
inner call demanding its own comments; and ``exp.Table(...)`` is an attribute
access, not a bare init of the state class ``Table``, so it is not flagged.
"""

import ast
import os

_STATE_NAMES = None


def _state_class_names():
    """Return (and cache) every StateModel subclass name in the engine.

    Computed by a STATIC scan of the federated_query source: parse every module,
    record each class and its base names, then take the transitive closure of
    classes that reach ``StateModel``. This deliberately does NOT import the
    package - the lint must run in any venv (including one where the package is
    not installed or its heavy deps are missing), and a flake8 worker should not
    pay to import the whole engine. These names are what FQ001 (bare init) and
    FQ002 (.create) guard.
    """
    global _STATE_NAMES
    if _STATE_NAMES is not None:
        return _STATE_NAMES
    import pathlib

    package = pathlib.Path(__file__).resolve().parent.parent / "federated_query"
    class_bases = {}
    for path in package.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                bases = set(filter(None, (_rightmost_name(base) for base in node.bases)))
                class_bases.setdefault(node.name, set()).update(bases)
    _STATE_NAMES = _closure_over_statemodel(class_bases)
    return _STATE_NAMES


def _closure_over_statemodel(class_bases):
    """Return every class name that transitively subclasses StateModel.

    ``class_bases`` maps a class name to the set of its base names. A class is a
    state class if it lists StateModel as a base or lists any known state class;
    iterate to a fixpoint so multi-level subclassing is covered.
    """
    state = set()
    changed = True
    while changed:
        changed = False
        for name, bases in class_bases.items():
            if name not in state and ("StateModel" in bases or bases & state):
                state.add(name)
                changed = True
    return state


def _rightmost_name(expr):
    """Return the trailing identifier of a Name / Attribute / Subscript, else None.

    ``Scan`` -> ``Scan``; ``physical.Scan`` -> ``Scan``; ``Generic[T]`` -> the
    base of the subscript. Lets a bare, module-qualified, or subscripted
    reference to a class be treated the same.
    """
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    if isinstance(expr, ast.Subscript):
        return _rightmost_name(expr.value)
    return None


class FedqConstructionChecker:
    """flake8 checker emitting FQ001 (bare init) and FQ002 (create needs comment)."""

    name = "flake8-fedq"
    version = "1.0.0"

    def __init__(self, tree, filename, lines):
        """Store the parsed tree, the file name, and its physical source lines."""
        self.tree = tree
        self.filename = filename
        self.lines = lines

    def _in_scope(self):
        """True only for engine files under federated_query/ that are not tests."""
        path = "/" + self.filename.replace(os.sep, "/")
        return "/federated_query/" in path and "/tests/" not in path

    def _comments_above(self, lineno):
        """Count contiguous full-line comments directly above 1-indexed lineno.

        A blank or code line breaks the run, so the comments must sit
        immediately above the statement being defended.
        """
        count = 0
        index = lineno - 2
        while 0 <= index < len(self.lines):
            if self.lines[index].strip().startswith("#"):
                count += 1
                index -= 1
            else:
                break
        return count

    def _parents(self):
        """Map each AST node to its parent, so a call can climb to its statement."""
        parents = {}
        for node in ast.walk(self.tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        return parents

    def _enclosing_statement(self, node, parents):
        """Return the nearest enclosing ast.stmt for a node (itself if a stmt)."""
        current = node
        while current is not None and not isinstance(current, ast.stmt):
            current = parents.get(current)
        return current

    def _classify(self, node, names):
        """Return (code, class_name) if the call is a guarded construction site."""
        func = node.func
        if isinstance(func, ast.Name) and func.id in names:
            return "FQ001", func.id
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "create"
            and _rightmost_name(func.value) in names
        ):
            return "FQ002", _rightmost_name(func.value)
        return None

    def _message(self, code, name):
        """Render the human-facing message for a violation code."""
        if code == "FQ001":
            return (
                f"FQ001 bare init {name}(...) is forbidden; use {name}.create(...) "
                f"or .model_copy(update=...), or justify the bare init with >=2 "
                f"comment lines directly above the statement"
            )
        return f"FQ002 {name}.create(...) needs >=2 comment lines directly above the statement"

    def run(self):
        """Yield one flake8 finding per undefended construction statement."""
        if not self._in_scope():
            return
        names = _state_class_names()
        parents = self._parents()
        reported = set()
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Call):
                continue
            classified = self._classify(node, names)
            if classified is None:
                continue
            code, name = classified
            statement = self._enclosing_statement(node, parents)
            line = statement.lineno if statement is not None else node.lineno
            if self._comments_above(line) >= 2:
                continue
            if (line, code) in reported:
                continue
            reported.add((line, code))
            yield line, 0, self._message(code, name), type(self)
