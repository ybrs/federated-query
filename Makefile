# Federated Query Engine - developer tasks
#
# The PostgreSQL-backed tests (tests/e2e_decorrelation, tests/test_datasources)
# need a running PostgreSQL. The download/start/stop targets below provision a
# self-contained instance with no system install. See README-test-harness-setup.md.

# Where python and its installed tools come from. An activated venv
# (VIRTUAL_ENV) supplies them; otherwise they resolve on PATH. No venv path or
# Python version is written down here - a venv that moves or gets rebuilt on a
# different interpreter keeps working, and there is no stale location to point
# these targets at an environment missing the dependencies.
# Override: make PYTHON=/path/to/python.
PYBIN  ?= $(if $(VIRTUAL_ENV),$(VIRTUAL_ENV)/bin/,)
PYTHON ?= $(PYBIN)python3

.PHONY: build dev-build check-python setup download_postgres pg-start pg-stop test test-no-db install lint lint-ascii lint-python fq-lint duckdb-lib

# Download the prebuilt PostgreSQL binaries into ./postgres-17 (one-time).
download_postgres:
	./scripts/download_postgresql.sh

# Start PostgreSQL in the background (initializes the cluster on first run).
pg-start:
	./scripts/run-postgres.sh

# Stop the background PostgreSQL instance.
pg-stop:
	./scripts/stop-postgres.sh

# Build the WHOLE workspace in ONE command: fetch the prebuilt libduckdb, verify
# Python is usable (the fedq-py extension needs it), then compile everything.
# `make build` = release, `make dev-build` = debug.
build: duckdb-lib check-python
	cargo build --release

dev-build: duckdb-lib check-python
	cargo build

# Fail with a clear message if pyo3 (the fedq-py extension) has no usable Python
# interpreter, checked in pyo3's own search order - instead of the cryptic
# "failed to run the Python interpreter" error deep in the build.
check-python:
	@py=""; \
	if [ -n "$$PYO3_PYTHON" ]; then py="$$PYO3_PYTHON"; \
	elif [ -n "$$VIRTUAL_ENV" ]; then py="$$VIRTUAL_ENV/bin/python"; \
	elif [ -n "$$CONDA_PREFIX" ]; then py="$$CONDA_PREFIX/bin/python"; \
	else py="$$(command -v python3 || true)"; fi; \
	if [ -z "$$py" ] || [ ! -x "$$py" ]; then \
		echo "ERROR: no usable Python interpreter for the fedq-py extension (pyo3)." >&2; \
		echo "  tried: $${py:-<none>}   (VIRTUAL_ENV=$${VIRTUAL_ENV:-<unset>})" >&2; \
		echo "  fix: install python3, activate a real venv, or set PYO3_PYTHON=\$$(command -v python3)" >&2; \
		exit 1; \
	fi; \
	echo "pyo3 will use Python: $$py"

setup: duckdb-lib
	@echo "Setup done. Build with: make build (release) or make dev-build (debug)."

# Fetch the official prebuilt libduckdb the workspace links against (one-time;
# idempotent). DuckDB is NEVER compiled inside cargo - see .cargo/config.toml.
duckdb-lib:
	./scripts/setup-duckdb-lib.sh

# Install the git hooks (idempotent; also runs automatically on session start
# via the SessionStart hook in .claude/settings.json). pre-commit = the
# semantic comment gate (a haiku judge reviews every staged .rs/.py comment
# against scripts/comment-gate/RUBRIC.md).
hook-install:
	./scripts/install-hooks.sh

# Install the test suite's dependencies into the active environment. There is no
# package to install any more: the engine is Rust, reached through the compiled
# fedq extension, and these are what the harnesses themselves import.
install:
	$(PYTHON) -m pip install -r requirements.txt

# Run the full test suite (expects PostgreSQL running via `make pg-start`).
test:
	$(PYTHON) -m pytest -q

# Run only the tests that do not require PostgreSQL. The federated corpus reads
# the flag itself and skips its PostgreSQL placements visibly, rather than the
# suite being narrowed by path.
test-no-db:
	FEDQ_E2E_SKIP_PG=1 $(PYTHON) -m pytest -q

# Run all linters (same checks the PostToolUse hook runs per edited file).
# fq-lint is the Rust construction linter; lint-python is flake8 over every
# Python file in the repository.
lint: lint-ascii lint-python fq-lint

# Codepoint rule: fail on any character above U+00FF anywhere in the repo.
# --error makes semgrep exit non-zero when it finds a match.
lint-ascii:
	$(PYBIN)semgrep --config .semgrep.yml --error --quiet .

# flake8 over EVERY Python file in the repository. The target names no
# directory on purpose: `.` plus the excludes in .flake8 is the whole rule, so a
# folder added later is covered without anyone remembering to list it here.
lint-python:
	$(PYBIN)flake8 .

# The engine construction linter (ports lint/flake8_fedq.py): forbids re-listing
# every field to rebuild a plan/expr node (use `Node { field: new, ..base }` or
# clone-and-mutate), and forbids name-string-matching relation membership.
fq-lint:
	cargo run -q -p fq-lint -- .
