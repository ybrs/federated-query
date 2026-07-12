"""The semantic comment gate's judge driver (invoked by gate.sh).

Extracts every comment block from the STAGED .rs/.py files, splits them into
model-context-sized chunks on block boundaries, has `claude -p --model haiku`
judge each chunk against RUBRIC.md, and aggregates the verdicts.

Exit 0: nothing staged, or every chunk judged clean.
Exit 1: violations (printed one per line) OR a judge call / JSON-parse failure
        (printed raw) - a broken judge blocks the commit loudly, it never waves
        one through.
"""

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import extract

# Characters of comment text per judge call. Well under the model's context
# once the rubric and instruction framing are added.
CHUNK_CHARS = 250_000


def _staged_blocks():
    """Every comment block of every staged .rs/.py file, rendered as text units."""
    units = []
    for path in extract._staged_source_files():
        text = extract._staged_content(path)
        if path.endswith(".py"):
            blocks = extract.extract_python_blocks(text)
        else:
            blocks = list(extract.extract_blocks(path, text))
        for start, lines in blocks:
            unit = "=== {0}:{1}\n{2}\n".format(path, start, "\n".join(lines))
            units.append(unit)
    return units


def _chunks(units):
    """Group block units into chunks of at most CHUNK_CHARS characters."""
    grouped = []
    current = []
    size = 0
    for unit in units:
        if current and size + len(unit) > CHUNK_CHARS:
            grouped.append("".join(current))
            current, size = [], 0
        current.append(unit)
        size += len(unit)
    if current:
        grouped.append("".join(current))
    return grouped


def _judge(rubric, chunk):
    """One judge call; returns the parsed violation list or dies loudly."""
    prompt = "{0}\n\nReview these comment blocks:\n\n{1}".format(rubric, chunk)
    result = subprocess.run(
        ["claude", "-p", "--model", "haiku"],
        input=prompt, capture_output=True, text=True,
    )
    raw = result.stdout.strip()
    if result.returncode != 0:
        print("COMMENT GATE: judge call failed (exit {0})".format(result.returncode))
        print(result.stderr.strip()[:2000])
        sys.exit(1)
    # The model sometimes wraps the array in a code fence or trailing prose;
    # decode the FIRST JSON array in the reply and ignore the wrapping. No
    # array at all = a malformed verdict = block.
    start = raw.find("[")
    if start < 0:
        print("COMMENT GATE: judge returned no JSON array; blocking. Raw reply:")
        print(raw[:2000])
        sys.exit(1)
    try:
        violations, _ = json.JSONDecoder().raw_decode(raw[start:])
    except ValueError:
        print("COMMENT GATE: judge returned undecodable JSON; blocking. Raw reply:")
        print(raw[:2000])
        sys.exit(1)
    return violations


def main():
    """Judge all staged comments in chunks; print violations and set exit code."""
    units = _staged_blocks()
    if not units:
        sys.exit(0)
    with open(os.path.join(HERE, "RUBRIC.md")) as handle:
        rubric = handle.read()
    chunks = _chunks(units)
    violations = []
    for index, chunk in enumerate(chunks):
        if len(chunks) > 1:
            print("comment gate: judging chunk {0}/{1} ...".format(index + 1, len(chunks)))
        violations.extend(_judge(rubric, chunk))
    if not violations:
        sys.exit(0)
    print("COMMENT GATE: {0} project-management comment(s) in staged files".format(len(violations)))
    print("(a comment describes the code as it is today - see scripts/comment-gate/RUBRIC.md)")
    for violation in violations:
        print("  {0}:{1}: {2!r} - {3}".format(
            violation.get("file"), violation.get("line"),
            violation.get("quote"), violation.get("reason")))
    sys.exit(1)


if __name__ == "__main__":
    main()
