#!/usr/bin/env bash
# Semantic comment gate, run BEFORE every git commit: every comment block in
# the STAGED .rs/.py files (Python: '#' runs + docstrings) is judged by a model
# against RUBRIC.md - a comment describes the code as it is today, never the
# project, its history, or its plans. Keyword lists cannot judge this; a model
# reads the semantics. Large staged sets are judged in chunks (gate.py).
#
# Exit 0: no staged source comments, or all judged clean.
# Exit 1: violations (printed) OR the judge failed - a broken gate blocks the
#         commit loudly rather than waving it through.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$here/gate.py"
