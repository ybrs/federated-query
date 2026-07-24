"""PreToolUse hook: a ticket cannot reach `done` over a red tree.

Fires on the taskq status tools. When the requested status is `done`, it runs
the whole project gate - `make lint`, `cargo test --workspace`, then the full
pytest e2e suite - and DENIES the tool call if any of them fails, handing the
failing output back so the model fixes the tree instead of closing the ticket
over it. Any other status passes straight through, so `in_progress`/`blocked`/
`todo` transitions stay instant.

A suite that cannot RUN denies too, naming what was missing. Missing toolchain,
missing venv, absent libduckdb, a database that is down: each would otherwise
turn a suite into a no-op or a wall of connection errors that reads like a
result. It is checked up front and reported as a failure.

Why this exists: per-file lint feedback only ever sees files the current agent
edited, so drift in untouched files accumulates invisibly. The ticket boundary
is the one place a whole-repo check is both cheap enough to afford and late
enough to be meaningful.

Exit 0 with a deny decision blocks the call; exit 0 with no output allows it.
A gate that cannot run its own tools DENIES rather than waving the ticket
through - a check that silently disappears is worse than no check.
"""

import json
import os
import subprocess
import sys

# Statuses that close work out. Everything else is a cheap transition.
_GATED_STATUSES = {"done"}

# Set to 1 to close a ticket over a red tree. The skip is reported in the
# decision output, so it can never be silent.
_OVERRIDE = "FQ_SKIP_DONE_GATE"


def _repo_root():
    """Return the project root (this file lives at <root>/.claude/hooks/)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _cargo_env():
    """Return an environment that can run cargo, or None if cargo is missing.

    A hook runs with whatever environment the session has, which may not be the
    interactive shell's. Resolve the cargo bin directory explicitly and pair it
    with the rustup home beside it, so a toolchain installed outside $HOME (or
    a $HOME that is not the installing user's) still works.
    """
    env = dict(os.environ)
    candidates = [
        os.path.join(os.path.expanduser("~"), ".cargo"),
        "/home/node/.cargo",
        "/usr/local/cargo",
    ]
    for base in candidates:
        binary = os.path.join(base, "bin", "cargo")
        if not os.path.isfile(binary):
            continue
        env["PATH"] = os.path.join(base, "bin") + os.pathsep + env.get("PATH", "")
        env["CARGO_HOME"] = base
        rustup = os.path.join(os.path.dirname(base), ".rustup")
        if os.path.isdir(rustup):
            env["RUSTUP_HOME"] = rustup
        return env
    return None


def _venv_python(root):
    """The interpreter that has pytest and the engine's test deps."""
    base = os.environ.get("VIRTUAL_ENV") or os.path.join(
        os.path.dirname(root), "venv-fedq"
    )
    return os.path.join(base, "bin", "python")


def _preconditions(root):
    """Return why the gate cannot run its suites, or None when it can.

    Each entry is something whose absence would otherwise turn a suite into a
    no-op or a wall of connection errors - the state that has to be reported as
    a failure rather than interpreted afterwards.
    """
    python = _venv_python(root)
    if not os.path.isfile(python):
        return f"No python venv at {python}: the e2e suite CANNOT RUN."
    probe = subprocess.run(
        [python, "-c", "import pytest"], capture_output=True, text=True
    )
    if probe.returncode != 0:
        return f"pytest is not installed in {python}: the e2e suite CANNOT RUN."
    if not os.path.isfile(os.path.join(root, ".duckdb-lib", "current", "libduckdb.so")):
        return (
            "No libduckdb at .duckdb-lib/current: the engine cannot load, so the "
            "e2e suite CANNOT RUN. Fix: make duckdb-lib"
        )
    pg_ready = os.path.join(root, "postgres-17", "bin", "pg_isready")
    if not os.path.isfile(pg_ready):
        return (
            "postgres-17/bin/pg_isready is missing, so the database state is "
            "UNKNOWN and the e2e suite cannot be trusted. Fix: make download_postgres"
        )
    up = subprocess.run(
        [pg_ready, "-h", "127.0.0.1", "-p", "5432"], capture_output=True, text=True
    )
    if up.returncode != 0:
        return (
            "PostgreSQL is not accepting connections: the e2e suite CANNOT RUN "
            "against it. Fix: make pg-start"
        )
    return None


def _run(label, command, root, env):
    """Run one gate step; return its output text on failure, else None."""
    completed = subprocess.run(
        command, cwd=root, env=env, capture_output=True, text=True
    )
    if completed.returncode == 0:
        return None
    tail = (completed.stdout + completed.stderr).strip().splitlines()
    return f"{label} FAILED ({' '.join(command)}):\n" + "\n".join(tail[-40:])


def _deny(reason):
    """Emit a PreToolUse deny decision carrying `reason` back to the model."""
    json.dump(
        {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
            }
        },
        sys.stdout,
    )
    return 0


def _requested_status(payload):
    """The status the tool call is trying to set, lowercased, or None."""
    status = (payload.get("tool_input") or {}).get("status")
    return status.strip().lower() if isinstance(status, str) else None


def main():
    """Gate a done-transition on lint + tests; allow everything else."""
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    if _requested_status(payload) not in _GATED_STATUSES:
        return 0
    if os.environ.get(_OVERRIDE) == "1":
        print(f"{_OVERRIDE}=1: closing this ticket WITHOUT running lint or tests.",
              file=sys.stderr)
        return 0
    root = _repo_root()
    env = _cargo_env()
    if env is None:
        return _deny(
            "Done-gate cannot run: no cargo toolchain found, so lint and tests "
            "could not be verified. Install the toolchain or set "
            f"{_OVERRIDE}=1 to close this ticket without the gate."
        )
    blocker = _preconditions(root)
    if blocker:
        return _deny(
            "This ticket cannot be marked done: the gate itself could not run.\n\n"
            + blocker
            + "\n\nA check that cannot run is a failure, not a pass."
        )
    failures = []
    for label, command in (
        ("Lint", ["make", "lint"]),
        ("Rust tests", ["cargo", "test", "--workspace"]),
        ("e2e suite", [_venv_python(root), "-m", "pytest", "-q", "-p", "no:cacheprovider"]),
    ):
        failure = _run(label, command, root, env)
        if failure:
            failures.append(failure)
            break
    if not failures:
        return 0
    return _deny(
        "This ticket cannot be marked done: the tree is red.\n\n"
        + "\n\n".join(failures)
        + "\n\nFix the failures above, then set the status again."
    )


if __name__ == "__main__":
    sys.exit(main())
