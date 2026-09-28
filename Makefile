# Federated Query Engine - developer tasks
#
# The PostgreSQL-backed tests (tests/e2e_decorrelation, tests/test_datasources)
# need a running PostgreSQL. The download/start/stop targets below provision a
# self-contained instance with no system install. See README-test-harness-setup.md.

# Every target uses only the currently activated virtualenv.
VENV   := $(VIRTUAL_ENV)
PYBIN  := $(VENV)/bin/
PYTHON := $(PYBIN)python3

.PHONY: build dev-build check-python setup download_postgres pg-start pg-stop test install venv lint lint-ascii lint-python fq-lint duckdb-lib fedq-server-image

# The container image the game-analytics demo's compose file runs. Overridable
# so the same Dockerfile can produce a differently named or tagged image.
FEDQ_SERVER_IMAGE ?= fedq-server:local

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
	PYO3_PYTHON=$(PYTHON) cargo build --release

dev-build: duckdb-lib check-python
	PYO3_PYTHON=$(PYTHON) cargo build

# Fail with a clear message if the fedq-py extension (pyo3) has no usable Python
# interpreter, instead of the cryptic "failed to run the Python interpreter"
# error deep in the build. The extension is loaded by the suite from the active
# virtualenv, so it must be built against that interpreter.
check-python:
	@if [ -z "$(VENV)" ]; then \
		echo "ERROR: no active Python virtualenv. Activate one before running make." >&2; \
		exit 1; \
	elif [ ! -x "$(PYTHON)" ]; then \
		echo "ERROR: no interpreter at $(PYTHON) for the fedq-py extension (pyo3)." >&2; \
		exit 1; \
	fi; \
	echo "pyo3 will use Python: $(PYTHON)"

setup: duckdb-lib
	@echo "Setup done. Build with: make build (release) or make dev-build (debug)."

# Fetch the official prebuilt libduckdb the workspace links against (one-time;
# idempotent). DuckDB is NEVER compiled inside cargo - see .cargo/config.toml.
duckdb-lib:
	./scripts/setup-duckdb-lib.sh

# Build the fedq-server image that vendor/game-analytics-demo runs under docker
# compose. It is built HERE and not there: the image needs this whole workspace
# as its context, while the demo's compose file must stay self-contained so the
# demo can move to its own repository. Run this once before the demo's
# `docker compose up`; without the image, compose stops at the engine service.
fedq-server-image:
	docker build \
		-f vendor/game-analytics-demo/docker/engine.Dockerfile \
		-t $(FEDQ_SERVER_IMAGE) .

# Install the git hooks (idempotent; also runs automatically on session start
# via the SessionStart hook in .claude/settings.json). pre-commit = the
# semantic comment gate (Codex gpt-6-luna at low reasoning effort reviews
# every staged .rs/.py comment against scripts/comment-gate/RUBRIC.md).
hook-install:
	./scripts/install-hooks.sh

# Bootstrap pip and install requirements into the active virtualenv.
venv:
	$(MAKE) install

# Install the test suite's dependencies into the active virtualenv. There is no
# install any more: the engine is Rust, reached through the compiled fedq
# extension, and these are what the harnesses themselves import.
install: check-python
	$(PYTHON) -m ensurepip --upgrade
	$(PYTHON) -m pip install -r requirements.txt

# Run the Python suite (expects PostgreSQL running via `make pg-start`). The
# whole suite is the only suite; ./run_all_tests.sh runs it alongside the
# linters and the Rust tests, which is what a change is judged by.
test: check-python
	. ./scripts/test-postgres-env.sh && \
		ensure_test_postgres "$(CURDIR)" && \
		$(PYTHON) -m pytest -q

# Run all linters (same checks the PostToolUse hook runs per edited file).
# fq-lint is the Rust construction linter; lint-python is flake8 over every
# Python file in the repository.
lint: lint-ascii lint-python fq-lint

# Codepoint rule: fail on any character above U+00FF anywhere in the repo.
# --error makes semgrep exit non-zero when it finds a match.
lint-ascii: check-python
	$(PYBIN)semgrep --config .semgrep.yml --error --quiet .

# flake8 over EVERY Python file in the repository. The target names no
# directory on purpose: `.` plus the excludes in .flake8 is the whole rule, so a
# folder added later is covered without anyone remembering to list it here.
lint-python: check-python
	$(PYBIN)flake8 .

# The engine construction linter (ports lint/flake8_fedq.py): forbids re-listing
# every field to rebuild a plan/expr node (use `Node { field: new, ..base }` or
# clone-and-mutate), and forbids name-string-matching relation membership.
fq-lint:
	cargo run -q -p fq-lint -- .
