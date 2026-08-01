#!/usr/bin/env bash
# Install the repo's git hooks into .git/hooks. Idempotent - safe to run on
# every session/environment start (a SessionStart hook in .claude/settings.json
# does exactly that), so a fresh checkout or rebuilt environment is always
# gated without manual setup.
#
# pre-commit runs EVERY check this project has on EVERY commit, with no
# file-type filter and no staged-file narrowing: lint, clippy, the Rust test
# suite, the e2e test suite, then the semantic comment gate. A commit that
# touches only README.md runs exactly the same checks as one that rewrites the
# optimizer. There is no fast path and no skip.
#
# The rule the hook exists to enforce: a check that cannot RUN is a failure,
# never a pass. Missing toolchain, no usable interpreter, a database that is not up, a
# suite that dies at import - each blocks the commit with a banner naming what
# could not run. Silence is what let the linters sit red for 17 days and the
# whole e2e suite sit dead for 2.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hook_dir="$repo_root/.git/hooks"

if [ ! -d "$repo_root/.git" ]; then
    echo "install-hooks: $repo_root is not a git checkout" >&2
    exit 1
fi

mkdir -p "$hook_dir"
cat > "$hook_dir/pre-commit" <<'EOF'
#!/usr/bin/env bash
# Installed by scripts/install-hooks.sh (re-run anytime; `make hook-install`).
#
# EVERY commit runs: make lint, cargo clippy, cargo test --workspace, the full
# pytest e2e suite, and the semantic comment gate. No file-type filter, no
# staged-only narrowing, no fast path for a docs-only change.
#
# A check that cannot run BLOCKS. It is never skipped and never assumed clean.
set -uo pipefail
root="$(git rev-parse --show-toplevel)"

# A hook does not inherit the interactive shell's environment, so every tool is
# resolved explicitly here. A tool that cannot be found is a blocking failure
# below, not a skipped step.
for base in "$HOME/.cargo" /home/node/.cargo /usr/local/cargo; do
    if [ -x "$base/bin/cargo" ]; then
        export PATH="$base/bin:$PATH" CARGO_HOME="$base"
        [ -d "$(dirname "$base")/.rustup" ] && export RUSTUP_HOME="$(dirname "$base")/.rustup"
        break
    fi
done

# The interpreter that runs the e2e suite is THE repository's ./venv, always -
# not $VIRTUAL_ENV and not python3 on PATH. Whatever a developer happens to have
# activated is a different environment with different installed versions, so
# honouring it means a commit is verified by an interpreter nobody else uses and
# the test harness was not built with.
python="$root/venv/bin/python3"

# Print a banner naming what failed and why the commit is refused, then exit.
block() {
    echo "" >&2
    echo "##############################################################################" >&2
    echo "##############################################################################" >&2
    echo "###                                                                        ###" >&2
    echo "###                     C O M M I T   B L O C K E D                        ###" >&2
    echo "###                                                                        ###" >&2
    echo "##############################################################################" >&2
    echo "##############################################################################" >&2
    echo "" >&2
    echo "  FAILED STEP: $1" >&2
    echo "" >&2
    echo "  $2" >&2
    echo "" >&2
    echo "  Every commit runs lint + cargo tests + e2e tests, on every file type." >&2
    echo "  A check that cannot RUN counts as FAILED, never as passed." >&2
    echo "" >&2
    echo "##############################################################################" >&2
    echo "" >&2
    exit 1
}

# Preconditions. Each one is a thing that, when absent, would otherwise let a
# suite silently not run.
command -v cargo >/dev/null 2>&1 \
    || block "toolchain check" "cargo not found. The Rust test suite CANNOT RUN, so nothing is verified."
[ -x "$python" ] \
    || block "toolchain check" "no interpreter at $python. The e2e suite CANNOT RUN. Fix: make venv"
"$python" -c "import pytest" >/dev/null 2>&1 \
    || block "toolchain check" "pytest is not installed in $python. The e2e suite CANNOT RUN. Fix: make install"
[ -f "$root/.duckdb-lib/current/libduckdb.so" ] \
    || block "toolchain check" "no libduckdb at .duckdb-lib/current. The engine cannot load and the e2e suite CANNOT RUN. Fix: make duckdb-lib"

# The e2e suite needs the PostgreSQL fixture. A database that is down would
# turn thousands of real assertions into connection errors, so it is a
# precondition, not a failure mode to interpret afterwards.
if [ -x "$root/postgres-17/bin/pg_isready" ]; then
    "$root/postgres-17/bin/pg_isready" -h 127.0.0.1 -p 5432 >/dev/null 2>&1 \
        || block "database check" "PostgreSQL is not accepting connections. The e2e suite CANNOT RUN against it. Fix: make pg-start"
else
    block "database check" "postgres-17/bin/pg_isready is missing, so the database state is UNKNOWN and the e2e suite cannot be trusted. Fix: make download_postgres"
fi

# --check only REPORTS; it never rewrites a file mid-commit. A hook that
# reformats the working tree cannot fix the commit being made - the files are
# already staged - so it would leave the tree dirty and the commit unformatted,
# which is exactly how 105 violations accumulated behind a "cargo fmt before
# commit" hook that appeared to be enforcing this.
cargo fmt --all --check --manifest-path "$root/Cargo.toml" \
    || block "cargo fmt --all --check" "Unformatted code above. Run: cargo fmt --all"

make -C "$root" lint \
    || block "make lint" "Lint violations above. Fix them."

cargo clippy --workspace --all-targets --manifest-path "$root/Cargo.toml" \
    || block "cargo clippy --workspace --all-targets" "Clippy errors above. Fix them."

# --test-threads caps the fan-out. Cargo runs the test binaries concurrently and
# each one defaults to a thread per core, so an unbounded run saturates the
# machine and the 100ms planning budget then kills plans that were never slow -
# the budget measures wall clock. The limit costs wall time and buys a signal
# that means what it says. The budget itself is not negotiable.
cargo test --workspace --manifest-path "$root/Cargo.toml" -- --test-threads=4 \
    || block "cargo test --workspace" "Rust test failures above. Fix them."

# The e2e suite drives the engine through tests/fedq.so, a symlink to the
# RELEASE build of fedq-py. Nothing else in this hook builds it, so without this
# step pytest exercises whatever binary was last built by hand - it ran against a
# six-commit-old engine before this was added, reporting green for code that was
# never executed. A suite testing a stale binary is a check that did not run.
cargo build --release -p fedq-py --manifest-path "$root/Cargo.toml" \
    || block "cargo build --release -p fedq-py" "The extension the e2e suite imports could not be built, so the suite would test a STALE engine."

# `-p no:cacheprovider` keeps the hook from writing .pytest_cache mid-commit.
# Collection errors exit non-zero here, so a suite that dies at import BLOCKS
# rather than reporting zero failures.
( cd "$root" && "$python" -m pytest -q -p no:cacheprovider ) \
    || block "pytest e2e suite" "e2e failures or collection errors above. A collection error means the suite did not run - that is a failure, not a pass."

exec "$root/scripts/comment-gate/gate.sh"
EOF
chmod +x "$hook_dir/pre-commit"
echo "install-hooks: pre-commit -> lint + clippy + cargo test + pytest e2e + comment gate (no exceptions)"
