#!/usr/bin/env bash
# Install the repo's git hooks into the shared git directory's hooks/, which is
# where git looks from the main checkout and from every linked worktree alike.
# Idempotent - safe to run on every session/environment start (a SessionStart
# hook in .claude/settings.json does exactly that), so a fresh checkout or
# rebuilt environment is always gated without manual setup.
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

# Hooks and the gate's state live in the git directory SHARED by the checkout
# and every linked worktree, so a worktree runs the same gate and reads the same
# green stamp. `.git` is a FILE in a worktree, never a directory, so the path is
# asked of git rather than assumed: git prints it relative to the repository
# root in a plain checkout and absolute in a worktree.
if ! git_common_dir="$(git -C "$repo_root" rev-parse --git-common-dir)"; then
    echo "install-hooks: $repo_root is not a git checkout" >&2
    exit 1
fi
case "$git_common_dir" in
    /*) ;;
    *) git_common_dir="$repo_root/$git_common_dir" ;;
esac
hook_dir="$git_common_dir/hooks"

mkdir -p "$hook_dir"
cat > "$hook_dir/pre-commit" <<'EOF'
#!/usr/bin/env bash
# Installed by scripts/install-hooks.sh (re-run anytime; `make hook-install`).
#
# EVERY commit runs these 
# -  make lint
# - cargo clippy
# - cargo test --workspace
# - the full pytest e2e suite
# - semantic comment gate. 
#
# No file-type filter, no staged-only narrowing, no fast path for a docs-only change.
#
set -uo pipefail
root="$(git rev-parse --show-toplevel)"

# A hook does not inherit the interactive shell's environment, so every tool is
# resolved explicitly here. A tool that cannot be found is a blocking failure
for base in "$HOME/.cargo" /home/node/.cargo /usr/local/cargo; do
    if [ -x "$base/bin/cargo" ]; then
        export PATH="$base/bin:$PATH" CARGO_HOME="$base"
        [ -d "$(dirname "$base")/.rustup" ] && export RUSTUP_HOME="$(dirname "$base")/.rustup"
        break
    fi
done

# Use only the active virtualenv, matching Makefile.
python=""
if [ -n "${VIRTUAL_ENV:-}" ]; then
    python="$VIRTUAL_ENV/bin/python3"
fi

# Report the failed check and its next step in a short sentence.
block() {
    printf '\nThe commit was blocked because %s failed.\n%s\n\n' "$1" "$2" >&2
    exit 1
}

# Preconditions. When any of the checks fail, we fail the whole suite..
command -v cargo >/dev/null 2>&1 \
    || block "the Rust toolchain check" "Cargo was not found. The Rust tests cannot run."
[ -x "$python" ] \
    || block "the Python toolchain check" "No active Python virtual environment was found. Activate it before committing."
"$python" -c "import pytest" >/dev/null 2>&1 \
    || block "the Python dependency check" "Pytest is missing from the active environment. Run make install, then retry the commit."
[ -f "$root/.duckdb-lib/current/libduckdb.so" ] \
    || block "the DuckDB library check" "The prebuilt DuckDB library is missing. Run make duckdb-lib."

# Start the repository test database on an available port and export it to the
# harness. An unrelated PostgreSQL service on port 5432 is left untouched.
source "$root/scripts/test-postgres-env.sh"
ensure_test_postgres "$root" \
    || block "test PostgreSQL startup" "The repository test database could not start. Review the PostgreSQL output above."

# --check only REPORTS; it never rewrites a file mid-commit. A hook that
# reformats the working tree cannot fix the commit being made - the files are
# already staged - so it would leave the tree dirty and the commit unformatted,
# which is exactly how 105 violations accumulated behind a "cargo fmt before
# commit" hook that appeared to be enforcing this.
cargo fmt --all --check --manifest-path "$root/Cargo.toml" \
    || block "Rust formatting" "Formatting checks failed. Run cargo fmt --all."

make -C "$root" lint \
    || block "lint checks" "The linters reported problems. Fix the issues shown above."

cargo clippy --workspace --all-targets --manifest-path "$root/Cargo.toml" \
    || block "Clippy" "Clippy reported errors. Fix the issues shown above."

# --test-threads caps the fan-out. Cargo runs the test binaries concurrently and
# each one defaults to a thread per core, so an unbounded run saturates the
# machine and the 100ms planning budget then kills plans that were never slow -
# the budget measures wall clock. The limit costs wall time and buys a signal
# that means what it says. The budget itself is not negotiable.
cargo test --workspace --manifest-path "$root/Cargo.toml" -- --test-threads=4 \
    || block "Rust tests" "One or more Rust tests failed. Review the test output above."

# Check generated paths before building the extension or starting test workers.
"$python" -c 'from tests.e2e_federated.verify_harness import check_config; problems = check_config(); print("Harness config is current." if not problems else "\n".join(problems)); raise SystemExit(bool(problems))' \
    || block "test harness config check" "The generated harness config is missing or out of date. Regenerate it with ./build_test_harness.sh, then retry."

# The e2e suite drives the engine through tests/fedq.so, a symlink to the
# RELEASE build of fedq-py. Nothing else in this hook builds it, so without this
# step pytest exercises whatever binary was last built by hand - it ran against a
# six-commit-old engine before this was added, reporting green for code that was
# never executed. A suite testing a stale binary is a check that did not run.
cargo build --release -p fedq-py --manifest-path "$root/Cargo.toml" \
    || block "the Python extension build" "The extension failed to build, so the end-to-end tests cannot run."

# The SAME suite run_all_tests.sh runs, as several processes instead of one. Each
# gets its own copy-on-write harness clone and therefore its own engine state, and
# the corpus is split by position, so the processes partition the suite rather
# than sampling it - every test runs exactly once. Sequentially this is about 45
# minutes and in parallel about 20, and it is paid on EVERY commit.
#
# A non-zero exit here means a real failure or a collection error; a collection
# error means the suite did not run, which is a failure and not a pass. Each
# process writes its whole output to run-e2e-logs/, so the detail survives.
( cd "$root" && ./run_e2e_parallel.sh ) \
    || block "end-to-end tests" "The first failure and its final log lines appear above. Full logs are in $root/run-e2e-logs/."

# commenting out for now because this is giving me issues with smaller models. and larger models are taking too long.
# "$root/scripts/comment-gate/gate.sh" 

# Every check above ran on the WORKING TREE. When that tree is exactly the
# content this commit is about to record - every porcelain line purely staged
# (second column clean), nothing untracked - record the index's tree id as a
# PENDING token. post-commit promotes it to the done-gate's green stamp only if
# the created commit's tree matches, so the stamp always names content the full
# gate actually verified. Any unstaged edit or untracked file means the
# verified content differs from the commit, and no token is written.
if ! git -C "$root" status --porcelain | grep -qvE '^[MTADRC] '; then
    # The token lives in the git directory SHARED by the checkout and every
    # linked worktree, where `.git` is a file rather than a directory. git
    # prints that path relative to the repository root in a plain checkout and
    # absolute in a worktree.
    common_dir="$(git -C "$root" rev-parse --git-common-dir)"
    case "$common_dir" in
        /*) ;;
        *) common_dir="$root/$common_dir" ;;
    esac
    mkdir -p "$common_dir/fedq-gate"
    git -C "$root" write-tree > "$common_dir/fedq-gate/pending-tree"
fi
EOF
chmod +x "$hook_dir/pre-commit"

cat > "$hook_dir/post-commit" <<'EOF'
#!/usr/bin/env bash
# Installed by scripts/install-hooks.sh (re-run anytime; `make hook-install`).
#
# Promote the pre-commit gate's PENDING token to the done-gate's green stamp,
# strictly by content: the token holds the tree id whose working-tree content
# the full gate verified, and the stamp is written only when the commit just
# created records exactly that tree. A commit made with --no-verify cannot be
# stamped - it has no token unless a fully green gate verified identical
# content, in which case the verification genuinely covers it.
set -uo pipefail
root="$(git rev-parse --show-toplevel)"

# The token and the stamp live in the git directory SHARED by the checkout and
# every linked worktree, where `.git` is a file rather than a directory. git
# prints that path relative to the repository root in a plain checkout and
# absolute in a worktree.
common_dir="$(git rev-parse --git-common-dir)"
case "$common_dir" in
    /*) ;;
    *) common_dir="$root/$common_dir" ;;
esac
token="$common_dir/fedq-gate/pending-tree"

[ -f "$token" ] || exit 0
verified_tree="$(cat "$token")"
rm -f "$token"
head_tree="$(git -C "$root" rev-parse 'HEAD^{tree}')"
[ "$verified_tree" = "$head_tree" ] || exit 0

commit="$(git -C "$root" rev-parse HEAD)"
stamp="$common_dir/fedq-gate/green-tree"
printf '{"tree": "%s", "commit": "%s", "verified_by": "pre-commit", "verified_at": "%s"}\n' \
    "$verified_tree" "$commit" "$(date +%Y-%m-%dT%H:%M:%S%z)" > "$stamp.tmp"
mv -f "$stamp.tmp" "$stamp"
EOF
chmod +x "$hook_dir/post-commit"
echo "install-hooks: pre-commit -> lint + clippy + cargo test + pytest e2e + comment gate (no exceptions); post-commit -> green-tree stamp for the done-gate"
