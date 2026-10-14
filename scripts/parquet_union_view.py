#!/usr/bin/env python3
"""Print a fedq view definition over the tables in a Parquet directory."""

import argparse
from pathlib import Path


def quote_identifier(name):
    """Quote one SQL identifier without interpreting punctuation as qualification."""
    if not name:
        raise ValueError("SQL identifiers must be nonempty")
    return '"' + name.replace('"', '""') + '"'


def union_branches(branches):
    """Build a balanced UNION ALL tree without increasing query depth per file."""
    if len(branches) == 1:
        return branches[0]
    middle = len(branches) // 2
    left = union_branches(branches[:middle])
    right = union_branches(branches[middle:])
    return "(" + left + ")\nUNION ALL\n(" + right + ")"


def view_sql(arguments):
    """Select explicit columns from each matching registered file table."""
    files = sorted(arguments.directory.glob(arguments.pattern))
    if not files:
        raise ValueError("no Parquet files match the supplied directory and pattern")
    columns = []
    for name in arguments.columns:
        columns.append(quote_identifier(name))
    prefix = quote_identifier(arguments.source) + "."
    prefix += quote_identifier(arguments.schema) + "."
    branches = []
    for path in files:
        branches.append(file_branch(path, prefix, columns))
    view = quote_identifier(arguments.view)
    return f"CREATE OR REPLACE VIEW {view} AS\n{union_branches(branches)};\n"


def file_branch(path, prefix, columns):
    """Select the requested columns from one registered Parquet file table."""
    table = prefix + quote_identifier(path.stem)
    return "SELECT " + ", ".join(columns) + " FROM " + table


def main():
    """Parse the source metadata options and print exactly one SQL statement."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--schema", default="main")
    parser.add_argument("--view", required=True)
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument("--columns", nargs="+", required=True)
    arguments = parser.parse_args()
    print(view_sql(arguments), end="")


if __name__ == "__main__":
    main()
