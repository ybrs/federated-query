#!/usr/bin/env bash
# Prepare a git worktree for agent builds so cargo reuses the main workspace's
# compiled dependencies instead of rebuilding ~3300 crates from scratch.
#
# Four steps, each REQUIRED for reuse to work:
#   1. Hardlink-copy the main target/ into the worktree (same artifacts, no disk).
#   2. Pin the DUCKDB [env] paths in the worktree's .cargo/config.toml to the
#      MAIN root's absolute path. Cargo hashes resolved [env] values into every
#      unit's fingerprint; the repo's `relative = true` entries resolve to a
#      different absolute path per tree, which alone forces a full rebuild.
#   3. Symlink .duckdb-lib to the main root's copy (binary rpaths are
#      $ORIGIN-relative, so the library must be present under the worktree).
#   4. Create ./venv in the worktree if absent (the suite requires it).
#
# The pinned config and the .duckdb-lib link are LOCAL, throwaway state and
# must NEVER be committed; agents stage files by explicit path only.
set -euo pipefail

main_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
worktree="${1:?usage: warm-worktree-agent.sh <worktree-path>}"
worktree="$(cd "$worktree" && pwd)"

if [ ! -f "$worktree/.git" ]; then
    echo "error: $worktree is not a linked git worktree (.git file missing)" >&2
    exit 1
fi
if [ ! -d "$main_root/target/release" ]; then
    echo "error: main workspace has no release target to warm from" >&2
    exit 1
fi
if [ ! -e "$main_root/.duckdb-lib/current" ]; then
    echo "error: main workspace has no .duckdb-lib/current (run scripts/setup-duckdb-lib.sh)" >&2
    exit 1
fi

echo "warming $worktree from $main_root"

rm -rf "$worktree/target"
cp -al "$main_root/target" "$worktree/target"

config="$worktree/.cargo/config.toml"
sed -i "s|{ value = \".duckdb-lib/current\", relative = true, force = true }|{ value = \"$main_root/.duckdb-lib/current\", force = true }|g" "$config"
pinned=$(grep -c "$main_root/.duckdb-lib/current" "$config")
if [ "$pinned" -ne 2 ]; then
    echo "error: expected to pin exactly 2 [env] entries in $config, pinned $pinned" >&2
    exit 1
fi

rm -rf "$worktree/.duckdb-lib"
ln -s "$main_root/.duckdb-lib" "$worktree/.duckdb-lib"

if [ ! -x "$worktree/venv/bin/python3" ]; then
    make -C "$worktree" venv
fi

echo "warmed: $(ls "$worktree"/target/release/deps/*.rlib | wc -l) release artifacts, [env] pinned, .duckdb-lib linked, venv ready"
echo "REMINDER: the pinned .cargo/config.toml and .duckdb-lib link must never be committed"
