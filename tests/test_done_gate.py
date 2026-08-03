"""The done-gate hook's own gating: it must fail CLOSED and find its state.

The hook is the last thing between a ticket reaching `done` and the full suite
having run over that ticket's tree, so the ways it can stop gating are pinned
here directly: a crash inside it, a linked worktree whose `.git` is a file
rather than a directory, and a stamp that would name content no single tree
describes.
"""

import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys

import pytest

HOOK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".claude",
    "hooks",
    "done_gate.py",
)


def _load_done_gate():
    """Import the hook by path; it is a standalone file in no package."""
    spec = importlib.util.spec_from_file_location("done_gate_under_test", HOOK_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


done_gate = _load_done_gate()


def _git(repo, *args):
    """Run one git command in `repo`; a failing command raises."""
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _write(path, text):
    """Write `text` to `path`, replacing whatever was there."""
    with open(path, "w") as handle:
        handle.write(text)


@pytest.fixture
def repo(tmp_path):
    """A throwaway git checkout holding one committed file."""
    path = str(tmp_path / "checkout")
    os.makedirs(path)
    _git(path, "init", "--quiet")
    _git(path, "config", "user.email", "gate@example.invalid")
    _git(path, "config", "user.name", "Gate Test")
    _write(os.path.join(path, "tracked.txt"), "one\n")
    _git(path, "add", "tracked.txt")
    _git(path, "commit", "--quiet", "--no-verify", "-m", "initial")
    return path


def _run_hook_rooted_at(directory, payload):
    """Run a copy of the hook rooted at `directory`; return its code and stdout.

    The hook resolves the repository from its own location, so copying it into
    `<directory>/.claude/hooks/` makes `directory` the root it gates. The git
    ceiling keeps git from discovering a repository above that root.
    """
    hooks_dir = os.path.join(directory, ".claude", "hooks")
    os.makedirs(hooks_dir)
    copied_hook = os.path.join(hooks_dir, "done_gate.py")
    shutil.copyfile(HOOK_PATH, copied_hook)
    environment = dict(os.environ)
    environment["GIT_CEILING_DIRECTORIES"] = directory
    completed = subprocess.run(
        [sys.executable, copied_hook],
        input=payload,
        cwd=directory,
        capture_output=True,
        text=True,
        env=environment,
    )
    return completed.returncode, completed.stdout


def test_a_status_other_than_done_is_allowed_with_no_decision(capsys):
    """Only a done-transition is gated; anything else passes straight through."""
    payload = json.dumps({"tool_input": {"status": "in_progress"}})
    assert done_gate._decide(payload) == 0
    assert capsys.readouterr().out == ""


def test_a_payload_that_is_not_json_is_denied(capsys):
    """An unparseable payload denies: the status it requests cannot be read.

    The hook fires only on the taskq status tools, so every payload it sees is
    a status change. One that cannot be parsed is a status change of unknown
    status, and letting it through would close a ticket over nothing verified.
    """
    assert done_gate._decide("not json at all") == 0
    decision = json.loads(capsys.readouterr().out)["hookSpecificOutput"]
    assert decision["permissionDecision"] == "deny"
    reason = decision["permissionDecisionReason"]
    assert "could not parse its stdin payload" in reason
    assert "15 bytes" in reason
    assert "JSONDecodeError" in reason


def test_a_crash_inside_the_gate_denies_and_reports_the_traceback(tmp_path):
    """A gate that cannot even locate its state DENIES rather than exiting 1.

    A hook blocks a call only by exiting 0 with a deny decision; every other
    nonzero exit is a non-blocking error the harness lets the call through. So a
    crash that merely propagated would close the ticket with nothing verified.
    The crash here is real: the hook is rooted where there is no git checkout.
    """
    directory = str(tmp_path / "not-a-checkout")
    os.makedirs(directory)
    payload = json.dumps({"tool_input": {"status": "done"}})
    code, stdout = _run_hook_rooted_at(directory, payload)
    assert code == 0, stdout
    decision = json.loads(stdout)["hookSpecificOutput"]
    assert decision["permissionDecision"] == "deny"
    assert "CRASHED" in decision["permissionDecisionReason"]
    assert "Traceback" in decision["permissionDecisionReason"]


def _explode_instead_of_gating(root):
    """Stand in for the gate itself, raising the way a broken step would."""
    raise OSError("the gate's state store is unreadable")


def test_any_exception_escaping_the_gate_becomes_a_deny(monkeypatch, capsys):
    """main() turns EVERY exception into a deny decision, never an exit code.

    The catch-all is what makes the hook fail closed. The harness reads a
    nonzero exit as a non-blocking error and runs the tool call anyway, so an
    exception allowed to propagate would close a ticket over an unverified tree.
    """
    payload = json.dumps({"tool_input": {"status": "done"}})
    monkeypatch.setattr(done_gate, "_gate_done_transition", _explode_instead_of_gating)
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    assert done_gate.main() == 0
    decision = json.loads(capsys.readouterr().out)["hookSpecificOutput"]
    assert decision["permissionDecision"] == "deny"
    assert "state store is unreadable" in decision["permissionDecisionReason"]


def test_the_gate_directory_is_shared_by_a_checkout_and_its_worktree(repo, tmp_path):
    """A linked worktree resolves to the SAME gate directory as its checkout.

    A worktree's `.git` is a file, so a path built as `<root>/.git/...` raises
    there and the gate crashes on every done-transition from a worktree. Sharing
    is also the correct meaning: the stamp names tree CONTENT, which is the same
    content whichever checkout observes it.
    """
    linked = str(tmp_path / "linked")
    _git(repo, "worktree", "add", "--quiet", linked)
    assert os.path.isfile(os.path.join(linked, ".git"))
    shared = os.path.realpath(os.path.join(repo, ".git", "fedq-gate"))
    assert os.path.realpath(done_gate._gate_dir(repo)) == shared
    assert os.path.realpath(done_gate._gate_dir(linked)) == shared


def test_a_green_run_over_unmoved_content_stamps_the_tree_it_verified(repo):
    """The tree snapshotted before the run is stamped when it is still current."""
    verified_tree = done_gate._content_snapshot(repo)
    assert verified_tree is not None
    done_gate._stamp_if_content_unchanged(repo, verified_tree)
    assert done_gate._stamped_tree(repo) == verified_tree


def test_a_commit_landing_during_the_run_leaves_no_stamp(repo):
    """Content that moves mid-run is stamped by nothing: no tree describes it.

    The gate's lock serializes done-transitions, not commits, so another session
    can commit while the suite runs. The suite then executed partly over one
    content and partly over another, and stamping the tree found afterwards
    would mark bytes green that were never verified whole.
    """
    verified_tree = done_gate._content_snapshot(repo)
    _write(os.path.join(repo, "tracked.txt"), "two\n")
    _git(repo, "commit", "--quiet", "--no-verify", "-a", "-m", "landed mid-gate")
    assert done_gate._content_snapshot(repo) != verified_tree
    done_gate._stamp_if_content_unchanged(repo, verified_tree)
    assert done_gate._stamped_tree(repo) is None


def test_a_checkout_left_dirty_by_the_run_is_never_stamped(repo):
    """An untracked file means the content is not one tree, so nothing is stamped."""
    verified_tree = done_gate._content_snapshot(repo)
    _write(os.path.join(repo, "stray.txt"), "untracked\n")
    done_gate._stamp_if_content_unchanged(repo, verified_tree)
    assert done_gate._stamped_tree(repo) is None


def test_a_stamp_satisfies_only_the_content_it_names(repo):
    """The stamped tree skips the re-run; any later edit stops satisfying it."""
    done_gate._stamp_if_content_unchanged(repo, done_gate._content_snapshot(repo))
    assert done_gate._stamp_satisfies(repo) is True
    _write(os.path.join(repo, "tracked.txt"), "edited\n")
    assert done_gate._stamp_satisfies(repo) is False
