# Federated Query Engine - developer tasks
#
# The PostgreSQL-backed tests (tests/e2e_decorrelation, tests/test_datasources)
# need a running PostgreSQL. The download/start/stop targets below provision a
# self-contained instance with no system install. See README-test-harness-setup.md.

PYTHON ?= python3
# Python virtualenv. An activated venv (VIRTUAL_ENV) is used automatically;
# otherwise falls back to the sibling ../venv-fedq. Override: make VENV=/path.
VENV   ?= $(if $(VIRTUAL_ENV),$(VIRTUAL_ENV),../venv-fedq)

.PHONY: build dev-build check-python setup download_postgres pg-start pg-stop test test-no-db install lint lint-ascii lint-construction fq-lint duckdb-lib

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

# Install the package and dependencies into the virtualenv.
install:
	$(VENV)/bin/pip install -r requirements.txt -e .

# Run the full test suite (expects PostgreSQL running via `make pg-start`).
test:
	$(VENV)/bin/python -m pytest -q

# Run only the tests that do not require PostgreSQL.
test-no-db:
	$(VENV)/bin/python -m pytest -q --ignore=tests/e2e_decorrelation

# Run all linters (same checks the PostToolUse hook runs per edited file).
# fq-lint is the Rust construction linter (the rewrite's equivalent of the Python
# lint-construction flake8 plugin).
lint: lint-ascii lint-construction fq-lint

# Codepoint rule: fail on any character above U+00FF anywhere in the repo.
# --error makes semgrep exit non-zero when it finds a match.
lint-ascii:
	$(VENV)/bin/semgrep --config .semgrep.yml --error --quiet .

# Construction rules: FQ001 (no unjustified bare init) and FQ002 (.create needs
# >=2 comment lines) over the engine, via the flake8 plugin in lint/.
lint-construction:
	$(VENV)/bin/flake8 --select=FQ federated_query

# The engine construction linter (ports lint/flake8_fedq.py): forbids re-listing
# every field to rebuild a plan/expr node (use `Node { field: new, ..base }` or
# clone-and-mutate), and forbids name-string-matching relation membership.
fq-lint:
	cargo run -q -p fq-lint -- .
