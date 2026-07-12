#!/usr/bin/env bash
# Install the repo's git hooks into .git/hooks. Idempotent - safe to run on
# every session/environment start (a SessionStart hook in .claude/settings.json
# does exactly that), so a fresh checkout or rebuilt environment is always
# gated without manual setup.
#
# pre-commit: the semantic comment gate (scripts/comment-gate/gate.sh) - a
# haiku judge reviews every staged .rs/.py comment against RUBRIC.md and blocks
# the commit on violations.
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
# The semantic comment gate must pass before any commit.
exec "$(git rev-parse --show-toplevel)/scripts/comment-gate/gate.sh"
EOF
chmod +x "$hook_dir/pre-commit"
echo "install-hooks: pre-commit -> scripts/comment-gate/gate.sh"
