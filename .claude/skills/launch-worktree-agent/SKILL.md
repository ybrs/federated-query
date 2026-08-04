---
name: launch-worktree-agent
description: Launch a subagent in a git worktree WITHOUT paying an hour-long cold Rust build. Use whenever spawning an agent that will compile this workspace in a worktree (fix agents, review agents, parallel feature work). Warms the worktree's target from the main workspace first - a cold worktree build is ~3300 crates and sat at 14% after 54 minutes under contention; a warmed one compiles only the ~17 workspace crates in minutes.
---

# Launching a build-capable agent in a worktree

A fresh worktree shares no build artifacts with the main workspace, and a
worktree under the main root sees TWO `.cargo/config.toml` files (its own
checkout plus the main one - cargo config discovery walks up the directory
tree), so the `[build]` rustflags apply twice and the doubled flags change
every unit's fingerprint: even a copied target directory rebuilds from
scratch. Both problems are fixed by warming the worktree before the agent
starts - the warm script hardlink-copies the target and deletes the
worktree's config copy so only the main one applies.

## Procedure

1. Create the worktree yourself (or let the Agent tool's `isolation: "worktree"`
   create one, then warm it before the agent needs to build - if the agent is
   already running a cold build, stop it first; a cold build is never worth
   finishing).

2. Warm it:

       ./scripts/warm-worktree-agent.sh <worktree-path>

   This hardlink-copies `target/`, deletes the worktree's checked-out
   `.cargo/config.toml`, symlinks `.duckdb-lib`, and creates `./venv`. After
   this, `cargo build --release` in the worktree compiles only the ~17
   workspace crates (measured: 1m34s, zero registry recompiles).

3. The agent prompt MUST include these worktree-state warnings:
   - `.cargo/config.toml` is intentionally DELETED in the worktree and
     `.duckdb-lib` is a symlink: never commit either state (no restoring the
     config, no `git add -A`, no `git add .` - stage by explicit path only).
   - The target is pre-warmed: if a build starts recompiling thousands of
     crates, something regressed - stop and check that the worktree's
     `.cargo/config.toml` is still absent instead of waiting.
   - `tests/fedq.so` resolves relative to the worktree, so it points at the
     worktree's own `target/release/libfedq_py.so` once built.
   - The e2e harness may need `./tests/e2e_federated/build_test_harness.sh`.

4. Concurrency: the machine has 12 cores and the gate assumes headroom. Two
   agents plus the main session is the practical ceiling; stagger heavy gate
   runs when possible. Check load with `uptime` before trusting any wall-clock
   number measured while agents build - contended numbers are 3-5x inflated
   and are not evidence.

## Integrating the agent's work

Agents leave their taskq ticket in_progress and their commits on the worktree
branch. Integrate by cherry-pick onto main (note: cherry-pick bypasses the
pre-commit gate, so the next main commit or done-transition re-verifies), then
compare branches by patch-id (`git cherry main <branch>`) before deleting the
worktree and branch. Never delete a worktree without checking
`git -C <worktree> status` for uncommitted work first.
