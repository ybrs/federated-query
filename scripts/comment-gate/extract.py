"""Extract every comment block from Rust and Python sources, with file:line anchors.

Used by the semantic comment gate (see gate.sh): the extracted blocks are what
the model reviews. For .rs files a block is a run of consecutive comment lines
(`//`, `//!`, `///`) plus inline trailing comments; for .py files it is every
`#` comment run plus every module/class/function docstring. Each block:

    === <file>:<first-line>
    <comment text, one line per source line>

Usage:
  python3 extract.py <file.rs|file.py> [...]   extract from files on disk
  python3 extract.py --staged                  extract from the STAGED version
                                               of every staged .rs/.py file
"""

import ast
import io
import subprocess
import sys
import tokenize


def _comment_part(line):
    """The comment text of a source line, or None.

    Splits on the first `//` not inside a string literal (a double-quote count
    heuristic: an even number of unescaped quotes before the `//` means we are
    outside a string; raw strings with quotes are rare in comments' vicinity).
    """
    position = 0
    while True:
        position = line.find("//", position)
        if position < 0:
            return None
        prefix = line[:position]
        if prefix.count('"') - prefix.count('\\"') == 0 or prefix.count('"') % 2 == 0:
            return line[position:].strip()
        position += 2


def extract_blocks(name, text):
    """Yield (start_line, [comment lines]) blocks from one file's text."""
    block = []
    start = None
    for number, line in enumerate(text.splitlines(), start=1):
        comment = _comment_part(line)
        if comment is None:
            if block:
                yield start, block
                block, start = [], None
            continue
        if not block:
            start = number
        block.append(comment)
    if block:
        yield start, block


def _python_comment_blocks(text):
    """Yield (start_line, [lines]) for every `#` comment run in Python source."""
    block = []
    start = None
    previous_line = -1
    tokens = tokenize.generate_tokens(io.StringIO(text).readline)
    for token in tokens:
        if token.type != tokenize.COMMENT:
            continue
        line_number = token.start[0]
        if block and line_number != previous_line + 1:
            yield start, block
            block, start = [], None
        if not block:
            start = line_number
        block.append(token.string)
        previous_line = line_number
    if block:
        yield start, block


def _python_docstrings(text):
    """Yield (start_line, [lines]) for every module/class/function docstring."""
    tree = ast.parse(text)
    nodes = [tree]
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            nodes.append(node)
    for node in nodes:
        docstring = ast.get_docstring(node, clean=False)
        if docstring is None:
            continue
        body_first = node.body[0]
        yield body_first.lineno, docstring.splitlines() or [""]


def extract_python_blocks(text):
    """All comment blocks and docstrings of one Python file, in line order."""
    blocks = list(_python_comment_blocks(text))
    blocks.extend(_python_docstrings(text))
    blocks.sort(key=lambda item: item[0])
    return blocks


def _staged_source_files():
    """Repo-relative staged .rs/.py paths (Added/Copied/Modified/Renamed)."""
    output = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True, text=True, check=True,
    ).stdout
    files = []
    for line in output.splitlines():
        if line.endswith(".rs") or line.endswith(".py"):
            files.append(line)
    return files


def _staged_content(path):
    """The staged (index) content of one file."""
    return subprocess.run(
        ["git", "show", ":" + path], capture_output=True, text=True, check=True
    ).stdout


def main():
    """Print every comment block of the requested files to stdout."""
    if sys.argv[1:] == ["--staged"]:
        sources = []
        for path in _staged_source_files():
            sources.append((path, _staged_content(path)))
    else:
        sources = []
        for path in sys.argv[1:]:
            with open(path) as handle:
                sources.append((path, handle.read()))
    for name, text in sources:
        if name.endswith(".py"):
            blocks = extract_python_blocks(text)
        else:
            blocks = extract_blocks(name, text)
        for start, lines in blocks:
            print("=== {0}:{1}".format(name, start))
            for line in lines:
                print(line)


if __name__ == "__main__":
    main()
