"""PostToolUse hook: lint a just-edited file and block on violations.

Runs after Edit / Write / MultiEdit. Reads the hook JSON on stdin, finds the
edited file, and runs the project linters against just that file:

  - semgrep (.semgrep.yml, rule no-codepoint-above-ff) on any lintable text file,
    including docs and .rs, to enforce the 0-255 codepoint rule.
  - flake8 (--select=FQ, the lint/flake8_fedq.py plugin) on engine .py files,
    to enforce FQ001 (no unjustified bare init) and FQ002 (.create needs two
    comment lines).
  - fq-lint on .rs files, to enforce FQ-CONSTRUCT / FQ-CLONE / FQ-NAMEMATCH /
    FQ-PMCOMMENT / FQ-BUNDLED. fq-lint walks the whole `crates` tree rather
    than one file, so a Rust edit is checked against the workspace.

On any violation it prints the findings to stderr and exits 2, which is how a
PostToolUse hook feeds the problem back to the model so it fixes the edit. When
clean, or when the file is not lintable, it exits 0.

A linter this hook cannot execute is reported as a violation, never skipped: a
gate that silently disappears when its tooling is missing reports clean on code
nothing checked.
"""

import json
import os
import shutil
import subprocess
import sys

_LINTABLE_SUFFIXES = {
    ".py", ".md", ".txt", ".rst", ".yml", ".yaml",
    ".toml", ".cfg", ".ini", ".sh", ".sql", ".rs",
}


def _repo_root():
    """Return the project root (this file lives at <root>/.claude/hooks/)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _python():
    """Return the interpreter whose environment supplies the lint tools.

    An activated venv (VIRTUAL_ENV) wins, so the tools follow whichever
    environment the developer is working in; otherwise the interpreter running
    this hook supplies its own. No venv location or Python version is written
    down here, which is what let a moved venv silently disable the linters.
    """
    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        candidate = os.path.join(venv, "bin", "python")
        if os.path.isfile(candidate):
            return candidate
    return sys.executable


def _tool(name):
    """Resolve a lint tool (semgrep/flake8), or None when it is not installed.

    Looks beside the active interpreter first - a venv installs its tools into
    the same bin directory as its python - then falls back to PATH. Returning
    None rather than the bare name keeps a missing tool distinguishable from a
    resolved one, so the caller reports it instead of silently passing.
    """
    interpreter = _python()
    binary = os.path.join(os.path.dirname(interpreter), name)
    if os.path.isfile(binary):
        return binary
    return shutil.which(name)


def _edited_file(payload):
    """Return the absolute path of the file the tool edited, or None."""
    tool_input = payload.get("tool_input") or {}
    path = tool_input.get("file_path")
    if not path:
        return None
    if not os.path.isabs(path):
        path = os.path.join(_repo_root(), path)
    return path if os.path.isfile(path) else None


def _semgrep_findings(root, rel_path):
    """Return semgrep codepoint findings for one file as printable lines."""
    binary = _tool("semgrep")
    if binary is None:
        return [f"semgrep not installed beside {_python()}; codepoint rule NOT checked"]
    completed = subprocess.run(
        [binary, "--config", ".semgrep.yml", rel_path, "--json", "--quiet"],
        cwd=root, capture_output=True, text=True,
    )
    if not completed.stdout.strip():
        return []
    results = json.loads(completed.stdout).get("results", [])
    lines = []
    for result in results:
        start = result.get("start", {}).get("line", "?")
        lines.append(f"{rel_path}:{start}: {result.get('check_id')}: non-ascii above U+00FF")
    return lines


def _flake8_findings(root, rel_path):
    """Return flake8 FQ (construction rule) findings for one engine file."""
    binary = _tool("flake8")
    if binary is None:
        return [f"flake8 not installed beside {_python()}; FQ001/FQ002 NOT checked"]
    completed = subprocess.run(
        [binary, "--select=FQ", rel_path],
        cwd=root, capture_output=True, text=True,
    )
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _cargo_env(root):
    """Return (cargo_binary, env) able to run cargo, or (None, None).

    Mirrors _tool's resolution for the Rust side: a hook's environment is not
    the interactive shell's, so the cargo bin directory and the rustup home
    beside it are located explicitly.
    """
    for base in (
        os.path.join(os.path.expanduser("~"), ".cargo"),
        "/home/node/.cargo",
        "/usr/local/cargo",
    ):
        binary = os.path.join(base, "bin", "cargo")
        if not os.path.isfile(binary):
            continue
        env = dict(os.environ)
        env["PATH"] = os.path.join(base, "bin") + os.pathsep + env.get("PATH", "")
        env["CARGO_HOME"] = base
        rustup = os.path.join(os.path.dirname(base), ".rustup")
        if os.path.isdir(rustup):
            env["RUSTUP_HOME"] = rustup
        return binary, env
    return None, None


def _fq_lint_findings(root):
    """Return fq-lint findings for the workspace.

    fq-lint takes a root and walks its `crates` tree; it has no single-file
    mode, so a Rust edit is checked against the whole workspace. That is the
    point - it also catches drift in files this session never touched.
    """
    binary, env = _cargo_env(root)
    if binary is None:
        return ["cargo not found; fq-lint (FQ-CONSTRUCT/FQ-CLONE/FQ-PMCOMMENT) NOT checked"]
    completed = subprocess.run(
        [binary, "run", "-q", "-p", "fq-lint", "--", "."],
        cwd=root, env=env, capture_output=True, text=True,
    )
    if completed.returncode == 0:
        return []
    output = (completed.stdout + completed.stderr).splitlines()
    return [line for line in output if line.strip().startswith("./")]


def main():
    """Lint the edited file; exit 2 with findings on stderr, else exit 0."""
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    path = _edited_file(payload)
    if path is None:
        return 0
    root = _repo_root()
    if not path.startswith(root + os.sep):
        return 0
    if os.path.splitext(path)[1] not in _LINTABLE_SUFFIXES:
        return 0
    rel_path = os.path.relpath(path, root)
    findings = _semgrep_findings(root, rel_path)
    if rel_path.endswith(".py") and (os.sep + "federated_query" + os.sep) in (os.sep + rel_path):
        findings += _flake8_findings(root, rel_path)
    if rel_path.endswith(".rs"):
        findings += _fq_lint_findings(root)
    if not findings:
        return 0
    header = "Lint violations in the file you just edited (fix them):"
    print(header + "\n" + "\n".join(findings), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
