#!/usr/bin/env python
"""Extract DuckDB logical node manifests from checked-in C++ source."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


DEFAULT_DUCKDB_SOURCE_ROOT = Path(__file__).resolve().parent / "duckdb-src"
LOGICAL_HEADER_RELATIVE_PATH = Path(
    "src/include/duckdb/common/enums/logical_operator_type.hpp"
)
LOGICAL_STRINGS_RELATIVE_PATH = Path("src/common/enums/logical_operator_type.cpp")


@dataclass(frozen=True)
class DuckDBLogicalNode:
    """Holds one logical node from DuckDB source."""

    enum_name: str
    plan_name: str


@dataclass(frozen=True)
class DuckDBNodeManifest:
    """Holds extracted DuckDB node lists."""

    logical_nodes: tuple[DuckDBLogicalNode, ...]

    def to_json_dict(self) -> dict[str, object]:
        """Return a JSON-serializable manifest dictionary."""
        logical_nodes = []
        for node in self.logical_nodes:
            logical_nodes.append(
                {"enum_name": node.enum_name, "plan_name": node.plan_name}
            )
        return {"logical_nodes": logical_nodes}

    def logical_plan_names(self) -> tuple[str, ...]:
        """Return rendered logical plan names from the manifest."""
        names = []
        for node in self.logical_nodes:
            names.append(node.plan_name)
        return tuple(names)


def read_duckdb_node_manifest(source_root: Path) -> DuckDBNodeManifest:
    """Read DuckDB source files and return all extracted node lists."""
    enum_names = read_logical_enum_names(source_root)
    string_names = read_logical_string_names(source_root)
    logical_nodes = build_logical_nodes(enum_names, string_names)
    return DuckDBNodeManifest(logical_nodes)


def read_logical_enum_names(source_root: Path) -> tuple[str, ...]:
    """Read LogicalOperatorType enum entries from the DuckDB header."""
    header_path = source_root / LOGICAL_HEADER_RELATIVE_PATH
    text = header_path.read_text()
    enum_body = text.split("enum class LogicalOperatorType", 1)[1]
    enum_body = enum_body.split("{", 1)[1].split("};", 1)[0]
    names = []
    for line in enum_body.splitlines():
        name = logical_enum_name_from_line(line)
        if name:
            names.append(name)
    return tuple(names)


def logical_enum_name_from_line(line: str) -> Optional[str]:
    """Return one enum name from a C++ enum line."""
    cleaned_line = line.split("//", 1)[0].strip().rstrip(",")
    if not cleaned_line:
        return None
    if cleaned_line.startswith("//"):
        return None
    return cleaned_line.split("=", 1)[0].strip()


def read_logical_string_names(source_root: Path) -> dict[str, str]:
    """Read LogicalOperatorType string renderings from DuckDB source."""
    source_path = source_root / LOGICAL_STRINGS_RELATIVE_PATH
    text = source_path.read_text()
    pattern = r"case LogicalOperatorType::([A-Z0-9_]+):\s*return \"([^\"]+)\";"
    matches = re.findall(pattern, text)
    names = {}
    for enum_name, plan_name in matches:
        names[enum_name] = plan_name
    return names


def build_logical_nodes(
    enum_names: tuple[str, ...], string_names: dict[str, str]
) -> tuple[DuckDBLogicalNode, ...]:
    """Build rendered logical nodes from enum and string mappings."""
    nodes = []
    for enum_name in enum_names:
        plan_name = string_names.get(enum_name)
        if plan_name and plan_name != "INVALID":
            nodes.append(DuckDBLogicalNode(enum_name, plan_name))
    return tuple(nodes)


def format_text_manifest(manifest: DuckDBNodeManifest) -> str:
    """Return a human-readable manifest."""
    lines = []
    for node in manifest.logical_nodes:
        lines.append(f"{node.enum_name}\t{node.plan_name}")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract DuckDB node lists from checked-in C++ source."
    )
    parser.add_argument(
        "--duckdb-source-root",
        type=Path,
        default=DEFAULT_DUCKDB_SOURCE_ROOT,
        help="Path to the DuckDB source checkout.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the manifest extractor CLI."""
    args = parse_args()
    manifest = read_duckdb_node_manifest(args.duckdb_source_root)
    if args.format == "text":
        print(format_text_manifest(manifest))
        return
    print(json.dumps(manifest.to_json_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
