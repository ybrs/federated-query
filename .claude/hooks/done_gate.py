"""PreToolUse hook: a ticket cannot reach `done` over an unverified tree.

Fires on the taskq status tools. When the requested status is `done`, it
requires the FULL gate - `cargo fmt --all --check`, `make lint`, `cargo clippy
--workspace --all-targets`, `cargo test --workspace`, the release build of the
extension the e2e suite imports, then `./run_e2e_parallel.sh` (the whole
Python suite, partitioned) - to have passed on EXACTLY the tree the ticket is
being closed over, and DENIES the tool call otherwise.

"Exactly the tree" is content-keyed, never time-keyed: a green run stamps the
git TREE ID it verified into `.git/fedq-gate/green-tree` (written by this gate
after its own green run, and by the post-commit hook for a commit whose
pre-commit gate ran green on identical content). A done-transition skips the
re-run only when the working tree is porcelain-clean AND `HEAD^{tree}` equals
the stamped tree - the same bytes the suite already proved. Any edit, any
untracked file, any commit of other content misses the stamp and the whole
gate runs here, now.

There is NO override that skips VERIFICATION. The stamp is not an exception:
it is the same verification, already performed on identical content. A tree
with no matching stamp always runs the whole harness.

FAIL-CLOSED, in every direction a check could silently vanish:
- A suite that cannot RUN (missing toolchain, venv, libduckdb, database)
  denies, naming what was missing.
- The gate keeps an internal deadline BELOW the hook's own timeout and denies
  when it is exceeded, killing the step's whole process group - so the harness
  timeout can never expire the hook mid-suite and let the call through
  unverified, and no orphaned suite keeps running.
- Concurrent done-transitions serialize on a lock; the loser re-checks the
  stamp after waiting, so one green run satisfies a batch of closes.

Exit 0 with a deny decision blocks the call; exit 0 with no output allows it.
"""

import fcntl
import json
import os
import signal
import subprocess
import sys
import time

# Statuses that close work out. Everything else is a cheap transition.
_GATED_STATUSES = {"done"}

# The gate must always DECIDE before the harness's hook timeout (settings.json)
# can kill it, because a killed hook does not deny - the call would proceed
# unverified. This deadline is therefore strictly below that timeout.
_INTERNAL_BUDGET_SECONDS = 5100

_DEADLINE = time.monotonic() + _INTERNAL_BUDGET_SECONDS


def _remaining():
    """Seconds left before the gate must have decided."""
    return _DEADLINE - time.monotonic()


def _repo_root():
    """Return the project root (this file lives at <root>/.claude/hooks/)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _gate_dir(root):
    """The gate's state directory inside .git (never part of the tree)."""
    path = os.path.join(root, ".git", "fedq-gate")
    os.makedirs(path, exist_ok=True)
    return path


def _git(root, *args):
    """One git query; its stripped stdout, or None on failure."""
    completed = subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _worktree_is_clean(root):
    """Whether nothing differs from HEAD and nothing is untracked."""
    porcelain = _git(root, "status", "--porcelain")
    return porcelain == ""


def _head_tree(root):
    """The tree id of HEAD, or None outside a usable checkout."""
    return _git(root, "rev-parse", "HEAD^{tree}")


def _stamp_path(root):
    """Where the last green run's tree id is recorded."""
    return os.path.join(_gate_dir(root), "green-tree")


def _stamped_tree(root):
    """The tree id of the last verified-green run, or None."""
    try:
        handle = open(_stamp_path(root))
    except FileNotFoundError:
        return None
    with handle:
        record = json.load(handle)
    return record.get("tree")


def _write_stamp(root, tree, verified_by):
    """Record that the full gate passed on `tree`, replacing any older stamp."""
    record = {
        "tree": tree,
        "commit": _git(root, "rev-parse", "HEAD"),
        "verified_by": verified_by,
        "verified_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    path = _stamp_path(root)
    with open(path + ".tmp", "w") as handle:
        json.dump(record, handle)
    os.replace(path + ".tmp", path)


def _stamp_satisfies(root):
    """Whether the current content is exactly what a green run verified."""
    if not _worktree_is_clean(root):
        return False
    head_tree = _head_tree(root)
    if head_tree is None:
        return False
    return _stamped_tree(root) == head_tree


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
    """The repository's one interpreter, `./venv/bin/python3`, always.

    Not $VIRTUAL_ENV and not whatever python3 is on PATH: the gate verifies
    with the environment the project defines, or reports that it cannot.
    """
    return os.path.join(root, "venv", "bin", "python3")


def _preconditions(root):
    """Return why the gate cannot run its suites, or None when it can.

    Each entry is something whose absence would otherwise turn a suite into a
    no-op or a wall of connection errors - the state that has to be reported as
    a failure rather than interpreted afterwards.
    """
    python = _venv_python(root)
    if not os.path.isfile(python):
        return f"No interpreter at {python}: the e2e suite CANNOT RUN. Fix: make venv"
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
    return _postgres_blocker(root)


def _postgres_blocker(root):
    """Why the PostgreSQL fixture is unusable, or None when it is up."""
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
    """Run one gate step inside the deadline; return failure text, else None.

    The step runs in its own session so an overrun kills the WHOLE process
    group - a gate that dies leaving a suite running would both waste the
    machine and dirty shared fixtures behind the next run's back.
    """
    budget = _remaining()
    if budget <= 0:
        return f"{label} was not run: the gate's internal deadline had passed."
    process = subprocess.Popen(
        command,
        cwd=root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        output, _ = process.communicate(timeout=budget)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        return (
            f"{label} exceeded the gate's internal deadline "
            f"({_INTERNAL_BUDGET_SECONDS}s for the whole gate) and was killed."
        )
    if process.returncode == 0:
        return None
    tail = output.strip().splitlines()
    return f"{label} FAILED ({' '.join(command)}):\n" + "\n".join(tail[-40:])


def _gate_steps(root):
    """The full gate, in order. The e2e step is the SAME parallel runner the
    pre-commit hook and run_all_tests.sh use - one suite, one way to run it."""
    return (
        ("Formatting", ["cargo", "fmt", "--all", "--check"]),
        ("Lint", ["make", "lint"]),
        ("Clippy", ["cargo", "clippy", "--workspace", "--all-targets"]),
        ("Rust tests", ["cargo", "test", "--workspace", "--", "--test-threads=4"]),
        (
            "Extension build (the e2e suite imports tests/fedq.so)",
            ["cargo", "build", "--release", "-p", "fedq-py"],
        ),
        ("e2e suite (parallel)", ["bash", "./run_e2e_parallel.sh"]),
    )


def _run_full_gate(root):
    """Run every gate step; return the first failure's text, or None on green."""
    env = _cargo_env()
    if env is None:
        return (
            "No cargo toolchain found, so nothing could be verified. Install "
            "the toolchain - there is no way to close a ticket without the gate."
        )
    blocker = _preconditions(root)
    if blocker:
        return blocker
    for label, command in _gate_steps(root):
        failure = _run(label, command, root, env)
        if failure:
            return failure
    return None


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


def _acquire_lock(root):
    """Hold the gate lock, waiting inside the deadline; None if time ran out.

    One done-transition verifies at a time: two concurrent full runs would
    fight over the machine and the shared fixtures, and the loser's re-check
    of the stamp is how a batch of closes pays for one green run.
    """
    handle = open(os.path.join(_gate_dir(root), "lock"), "w")
    while _remaining() > 0:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            time.sleep(5)
    handle.close()
    return None


def _gate_done_transition(root):
    """Allow (None) or deny (reason) one done-transition, under the lock."""
    lock = _acquire_lock(root)
    if lock is None:
        return (
            "Another done-gate held the lock past this gate's whole deadline. "
            "Nothing was verified, so the transition is refused; try again."
        )
    with lock:
        if _stamp_satisfies(root):
            return None
        failure = _run_full_gate(root)
        if failure:
            return failure
        head_tree = _head_tree(root)
        if head_tree is not None and _worktree_is_clean(root):
            _write_stamp(root, head_tree, "done-gate")
        return None


def main():
    """Gate a done-transition on the verified tree; allow everything else."""
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    if _requested_status(payload) not in _GATED_STATUSES:
        return 0
    root = _repo_root()
    failure = _gate_done_transition(root)
    if failure is None:
        return 0
    return _deny(
        "This ticket cannot be marked done: the tree is not verified green.\n\n"
        + failure
        + "\n\nFix the failures above, then set the status again."
    )


if __name__ == "__main__":
    sys.exit(main())
