# comment-gate: semantic pre-commit review of Rust comments

Enforces the CLAUDE.md rule "comments describe the code, never the project" -
semantically, with a model, because keyword lists cannot judge whether a
sentence schedules work or states a fact.

- `extract.py` pulls every comment block (with `file:line` anchors) from .rs
  files and every `#` comment run + docstring from .py files, or from the
  STAGED versions with `--staged`.
- `RUBRIC.md` is the single judging standard (allowed vs violation, with the
  borderline cases spelled out).
- `gate.sh` runs on `git commit` (installed as `.git/hooks/pre-commit` by
  `scripts/install-hooks.sh`, which the SessionStart hook in
  `.claude/settings.json` runs automatically on every session, so a rebuilt
  environment is gated without manual setup; manual: `make hook-install`):
  extracts the staged comments, has `claude -p --model haiku` judge them
  against the rubric, and BLOCKS the commit listing each offending comment. A
  judge/parse failure also blocks - a broken gate never waves a commit through.

The fast mechanical layer (fq-lint FQ-PMCOMMENT: todo/fixme/milestone/commit-id
markers) still runs in `make fq-lint`; this gate is the semantic layer above it.

Run it manually on arbitrary files:

```
python3 scripts/comment-gate/extract.py crates/fq-bind/src/*.rs
```

and pipe the blocks with RUBRIC.md into `claude -p --model haiku` (exactly what
gate.sh does for the staged set).
