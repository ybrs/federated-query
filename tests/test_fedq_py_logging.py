"""The `fedq` extension module's opt-in log subscriber.

The subscriber `fedq.setup_logging()` installs is process-global, so the module
must NOT install one on import: a host application with its own logging setup
would find the engine had reconfigured it. These tests pin both halves of that
contract - silent until asked, emitting once asked - by running each case in a
fresh interpreter, since a process-global install cannot be undone in-process.
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent
SO_PATH = TESTS_DIR / "fedq.so"


def _require_extension():
    """Raise when the fedq extension is not built.

    An unbuilt extension is a broken workspace, not a passing run: skipping here
    reported success for a contract nothing had checked.
    """
    if not SO_PATH.exists():
        raise RuntimeError(
            "fedq.so is missing at " + str(SO_PATH) + "; build it with `make build`"
        )


def _run(config_path, body, user="audited"):
    """Run `body` in a fresh interpreter with the extension importable.

    `user` is per-call because the ACL store lives beside the config and
    persists across runs: reusing one name would fail the second CREATE USER
    with `already exists` and say nothing about logging.
    """
    _require_extension()
    script = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(TESTS_DIR)!r})
        import fedq
        {body}
        runtime = fedq.Runtime({str(config_path)!r})
        runtime.execute("CREATE USER {user} WITH PASSWORD 'pw'")
        """
    )
    return subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )


@pytest.fixture(name="config_path")
def config_fixture(tmp_path):
    """A minimal in-memory DuckDB config the extension can open."""
    path = tmp_path / "config.yaml"
    path.write_text(
        'datasources:\n  duck:\n    type: duckdb\n    path: ":memory:"\n'
    )
    return path


def test_importing_fedq_installs_no_subscriber(config_path):
    """Without the opt-in call the engine writes nothing to the host's streams."""
    result = _run(config_path, "")
    assert result.returncode == 0, result.stderr
    assert "fedq::audit" not in result.stderr
    assert "fedq::audit" not in result.stdout


def test_setup_logging_delivers_the_audit_record(config_path):
    """After the call the ACL audit record reaches stderr."""
    result = _run(config_path, 'fedq.setup_logging("info", False)')
    assert result.returncode == 0, result.stderr
    assert "fedq::audit" in result.stderr
    assert "audited" in result.stderr


def test_setup_logging_defaults_and_json_form(config_path):
    """The level defaults to info, and the structured form emits JSON objects."""
    default_level = _run(config_path, "fedq.setup_logging()", user="defaulted")
    assert default_level.returncode == 0, default_level.stderr
    assert "fedq::audit" in default_level.stderr

    structured = _run(
        config_path, 'fedq.setup_logging("info", True)', user="structured"
    )
    assert structured.returncode == 0, structured.stderr
    assert '"target":"fedq::audit"' in structured.stderr


def test_a_lower_level_silences_the_record(config_path):
    """The level is honored: at `error` the info-level audit line is dropped."""
    result = _run(config_path, 'fedq.setup_logging("error", False)')
    assert result.returncode == 0, result.stderr
    assert "fedq::audit" not in result.stderr
