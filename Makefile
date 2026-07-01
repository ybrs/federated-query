# Federated Query Engine - developer tasks
#
# The PostgreSQL-backed tests (tests/e2e_decorrelation, tests/test_datasources)
# need a running PostgreSQL. The download/start/stop targets below provision a
# self-contained instance with no system install. See README-test-harness-setup.md.

PYTHON ?= python3
# Python virtualenv. An activated venv (VIRTUAL_ENV) is used automatically;
# otherwise falls back to the sibling ../venv-fedq. Override: make VENV=/path.
VENV   ?= $(if $(VIRTUAL_ENV),$(VIRTUAL_ENV),../venv-fedq)

.PHONY: download_postgres pg-start pg-stop test test-no-db install lint lint-ascii lint-construction

# Download the prebuilt PostgreSQL binaries into ./postgres-17 (one-time).
download_postgres:
	./scripts/download_postgresql.sh

# Start PostgreSQL in the background (initializes the cluster on first run).
pg-start:
	./scripts/run-postgres.sh

# Stop the background PostgreSQL instance.
pg-stop:
	./scripts/stop-postgres.sh

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
lint: lint-ascii lint-construction

# Codepoint rule: fail on any character above U+00FF anywhere in the repo.
# --error makes semgrep exit non-zero when it finds a match.
lint-ascii:
	$(VENV)/bin/semgrep --config .semgrep.yml --error --quiet .

# Construction rules: FQ001 (no unjustified bare init) and FQ002 (.create needs
# >=2 comment lines) over the engine, via the flake8 plugin in lint/.
lint-construction:
	$(VENV)/bin/flake8 --select=FQ federated_query
