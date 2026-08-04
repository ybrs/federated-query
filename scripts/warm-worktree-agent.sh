#!/usr/bin/env bash
# Prepare a git worktree for agent builds so cargo reuses the main workspace's
# compiled dependencies instead of rebuilding ~3300 crates from scratch.
#
# Four steps, each REQUIRED for reuse to work:
#   1. Hardlink-copy the main target/ into the worktree (same artifacts, no
#      disk). Needs one filesystem, which the under-main-root requirement
#      already guarantees.
#   2. DELETE the worktree's checked-out .cargo/config.toml. A worktree under
#      the main root sees BOTH config files (cargo config discovery walks up
#      the directory tree), so the [build] rustflags apply twice - and the
#      doubled flags change every unit's fingerprint hash, forcing a full
#      rebuild. With the worktree copy gone, only the main config applies:
#      identical flags, identical fingerprints, and the DUCKDB [env] paths
#      resolve relative to the MAIN config's location, exactly as the warmed
#      artifacts were built.
#   3. Symlink .duckdb-lib to the main root's copy (binary rpaths are
#      $ORIGIN-relative, so the library must be present under the worktree at
#      run time).
#   4. Create ./venv in the worktree if absent (the suite requires it).
#
# The deleted config and the .duckdb-lib link are LOCAL, throwaway state and
# must NEVER be committed; agents stage files by explicit path only.
set -euo pipefail

main_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
worktree="${1:?usage: warm-worktree-agent.sh <worktree-path>}"
worktree="$(cd "$worktree" && pwd)"

if [ ! -f "$worktree/.git" ]; then
    echo "error: $worktree is not a linked git worktree (.git file missing)" >&2
    exit 1
fi
case "$worktree" in
    "$main_root"/*) ;;
    *)
        echo "error: $worktree is not under $main_root; the worktree must live" >&2
        echo "under the main root so cargo walks up to the ONE shared config" >&2
        exit 1
        ;;
esac
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

rm -f "$worktree/.cargo/config.toml"

rm -rf "$worktree/.duckdb-lib"
ln -s "$main_root/.duckdb-lib" "$worktree/.duckdb-lib"

if [ ! -x "$worktree/venv/bin/python3" ]; then
    make -C "$worktree" venv
fi

echo "warmed: $(ls "$worktree"/target/release/deps/*.rlib | wc -l) release artifacts, worktree config removed, .duckdb-lib linked, venv ready"
echo "REMINDER: the removed .cargo/config.toml and the .duckdb-lib link must never be committed"
