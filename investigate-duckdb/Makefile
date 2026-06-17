# ===========================================================================
# investigate-duckdb — reproducible setup + perf benchmarks
#
# What this builds and measures:
#   * a PostgreSQL fixture (duckpoc db: files 400k, categories 50k)
#   * a DuckDB fixture (analytics-poc.duckdb: access_logs 10M)
#   * two DuckDB binaries: stable v1.5.3 CLI, and main built from source
#     (duckdb-postgres + its pinned duckdb submodule)
#   * the timing table, the correctness check, and the per-query breakdown
#     that back performance-comparison.md / duckdb-pushdowns.md
#
# Prereqs (one-time, from the repo root):
#   make pg-start                     # start the bundled PostgreSQL on :5432
#   /workspace/venv-fedq has python + fedq + cmake + ninja (pip install cmake ninja)
#
# Quick start:
#   make all                          # data + both binaries + bench + verify
#   make bench                        # just the timing table
#   make verify                       # just the fedq-vs-DuckDB correctness check
# ===========================================================================

# ---- knobs (override on the command line, e.g. `make bench PG_DB=foo`) ----
PYTHON   ?= /workspace/venv-fedq/bin/python
FEDQ     ?= /workspace/venv-fedq/bin/fedq
PG_BIN   ?= /workspace/federated-query/postgres-17/bin
PG_HOST  ?= localhost
PG_PORT  ?= 5432
PG_USER  ?= postgres
PG_DB    ?= duckpoc
DUCKDB_VERSION ?= v1.5.3
# cmake/ninja for the source build live in the venv; put them on PATH.
VENV_BIN ?= /workspace/venv-fedq/bin

PSQL = $(PG_BIN)/psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER)

STABLE_BIN   = bin/duckdb-stable
MAIN_SRC     = pg-scanner-src
MAIN_BIN     = $(MAIN_SRC)/build/release/duckdb
ANALYTICS_DB = analytics-poc.duckdb

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
help: ## List the targets
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) \
	  | sed -E 's/:.*## /\t/' | sort | awk -F'\t' '{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

all: data binaries bench verify ## Everything: data + both DuckDB binaries + bench + verify

# ---- data fixtures --------------------------------------------------------
data: pg-data duck-data ## Build both data fixtures (PostgreSQL + DuckDB)

pg-check: ## Fail early if PostgreSQL is not reachable
	@$(PSQL) -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 \
	  || { echo "PostgreSQL not reachable on $(PG_HOST):$(PG_PORT) — run 'make pg-start' in the repo root"; exit 1; }

pg-data: pg-check ## Create the duckpoc db and load files + categories (schema.sql)
	@$(PSQL) -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$(PG_DB)'" | grep -q 1 \
	  || $(PSQL) -d postgres -c "CREATE DATABASE $(PG_DB)"
	$(PSQL) -d $(PG_DB) -f schema.sql
	@$(PSQL) -d $(PG_DB) -tAc "SELECT 'categories='||count(*) FROM categories UNION ALL SELECT 'files='||count(*) FROM files"

duck-data: $(STABLE_BIN) ## Build analytics-poc.duckdb (10M access_logs)
	rm -f $(ANALYTICS_DB)
	$(STABLE_BIN) $(ANALYTICS_DB) < build-analytics.sql
	@$(STABLE_BIN) $(ANALYTICS_DB) -c "SELECT 'access_logs='||count(*) FROM access_logs"

# ---- DuckDB binaries ------------------------------------------------------
binaries: $(STABLE_BIN) $(MAIN_BIN) ## Get the stable CLI and build main from source

$(STABLE_BIN): ## Download the stable DuckDB CLI ($(DUCKDB_VERSION))
	mkdir -p bin
	curl -sL -o bin/_d.zip "https://github.com/duckdb/duckdb/releases/download/$(DUCKDB_VERSION)/duckdb_cli-linux-amd64.zip"
	cd bin && unzip -o -q _d.zip && mv -f duckdb duckdb-stable && rm -f _d.zip
	$(STABLE_BIN) --version

# duckdb-postgres main vendors a duckdb submodule pinned to a compatible core.
# We clone it, apply the OAuth stub (its postgres_oauth.cpp needs the libpq-18
# API absent on PG-17 here), then `make release` builds the pinned core + the
# scanner. cmake/ninja come from the venv.
$(MAIN_SRC): ## Clone duckdb-postgres (main) with submodules
	git clone --recurse-submodules --shallow-submodules \
	  https://github.com/duckdb/duckdb-postgres.git $(MAIN_SRC)

$(MAIN_BIN): $(MAIN_SRC) ## Build the main DuckDB + postgres scanner from source
	cp patches/postgres_oauth.cpp.stub $(MAIN_SRC)/src/postgres_oauth.cpp
	cd $(MAIN_SRC) && PATH="$(VENV_BIN):$$PATH" GEN=ninja $(MAKE) release -j$$(nproc)
	$(MAIN_BIN) -unsigned -c "LOAD '$(CURDIR)/$(MAIN_SRC)/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension'; SELECT 'main ok';" 2>&1 | tail -1

# ---- benchmarks / outputs -------------------------------------------------
bench: pg-check ## Full timing sweep across all queries (stable / main / fedq)
	./bench-all.sh

bench-core: pg-check ## Timing for the 3 headline queries (Q4/Q7/Q1)
	./bench.sh

bench-drivers: pg-check ## COPY vs direct SQL: psycopg2 SELECT vs ADBC binary COPY
	$(PYTHON) bench-drivers.py

verify: pg-check ## Assert fedq and DuckDB return identical rows (Q4/Q7/Q1)
	./verify-correctness.sh

perfdoc: pg-check ## Regenerate the per-query breakdown section
	$(PYTHON) gen_perf_doc.py

pushdowns: pg-check ## Capture the SQL each engine sends to PostgreSQL (full text)
	./dump-fulltext.sh > /tmp/fulltext.md && echo "wrote /tmp/fulltext.md"

# ---- the streaming invariant test (the most important perf test) ----------
test-streaming: pg-check ## Run the Postgres-streaming contract test
	cd /workspace/federated-query && POSTGRES_DB=$(PG_DB) $(PYTHON) -m pytest tests/test_postgres_streaming.py -v

# ---- housekeeping ---------------------------------------------------------
clean-build: ## Remove the from-source build tree (keeps clones)
	rm -rf $(MAIN_SRC)/build duckdb-src/build

clean: ## Remove built binaries, the DuckDB fixture, and clones
	rm -rf bin $(ANALYTICS_DB) $(MAIN_SRC) duckdb-src

.PHONY: help all data binaries bench bench-core bench-drivers verify perfdoc \
        pushdowns pg-check pg-data duck-data test-streaming clean clean-build
