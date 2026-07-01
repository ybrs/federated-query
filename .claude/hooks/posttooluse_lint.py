"""PostToolUse hook: lint a just-edited file and block on violations.

Runs after Edit / Write / MultiEdit. Reads the hook JSON on stdin, finds the
edited file, and runs the project linters against just that file:

  - semgrep (.semgrep.yml, rule no-codepoint-above-ff) on any lintable text file,
    including docs, to enforce the 0-255 codepoint rule.
  - flake8 (--select=FQ, the lint/flake8_fedq.py plugin) on engine .py files,
    to enforce FQ001 (no unjustified bare init) and FQ002 (.create needs two
    comment lines).

On any violation it prints the findings to stderr and exits 2, which is how a
PostToolUse hook feeds the problem back to the model so it fixes the edit. When
clean, or when the file is not lintable, it exits 0.
"""

import json
import os
import subprocess
import sys

_LINTABLE_SUFFIXES = {
    ".py", ".md", ".txt", ".rst", ".yml", ".yaml",
    ".toml", ".cfg", ".ini", ".sh", ".sql",
}


def _repo_root():
    """Return the project root (this file lives at <root>/.claude/hooks/)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _tool(name):
    """Resolve a venv tool (semgrep/flake8) without hardcoding an absolute path.

    Prefers an activated venv (VIRTUAL_ENV), then the sibling ../venv-fedq or a
    repo-local ./venv, then the interpreter running this hook; falls back to the
    bare name on PATH. Mirrors the Makefile's venv resolution.
    """
    root = _repo_root()
    candidates = [
        os.environ.get("VIRTUAL_ENV"),
        os.path.join(os.path.dirname(root), "venv-fedq"),
        os.path.join(root, "venv"),
        os.path.dirname(os.path.dirname(sys.executable)),
    ]
    for base in candidates:
        if base:
            binary = os.path.join(base, "bin", name)
            if os.path.isfile(binary):
                return binary
    return name


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
    binary = os.path.join(os.path.dirname(sys.executable), "semgrep")
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
    binary = os.path.join(os.path.dirname(sys.executable), "flake8")
    completed = subprocess.run(
        [binary, "--select=FQ", rel_path],
        cwd=root, capture_output=True, text=True,
    )
    return [line for line in completed.stdout.splitlines() if line.strip()]


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
    if not findings:
        return 0
    header = "Lint violations in the file you just edited (fix them):"
    print(header + "\n" + "\n".join(findings), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
