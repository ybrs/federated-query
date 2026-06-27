"""Shared SQL-clause builders over the sqlglot AST.

One implementation each of the SELECT list, ORDER BY, GROUP BY / GROUPING SETS,
and LIMIT/OFFSET application. Every node that emits SQL - remote scans,
single-source pushdown, and the local DuckDB merge operators - composes these,
so clause rendering exists in exactly one place.
"""

from sqlglot import exp

from .expressions import expression_to_ast


def aliased_item(ast: exp.Expression, name) -> exp.Expression:
    """Wrap a SELECT expression in ``AS "name"`` unless ``name`` is falsy."""
    if not name:
        return ast
    return exp.alias_(ast, name, quoted=True)


def select_expressions(exprs, names, resolver) -> list:
    """Build aliased SELECT items from expressions and parallel output names."""
    items = []
    for index, expr in enumerate(exprs):
        name = names[index] if names and index < len(names) else None
        items.append(aliased_item(expression_to_ast(expr, resolver), name))
    return items


def order_by(keys, ascending, nulls, resolver):
    """Build an ``exp.Order`` from parallel key/ascending/nulls lists, or None."""
    if not keys:
        return None
    ordered = []
    for index, key in enumerate(keys):
        ordered.append(_ordered_key(index, key, ascending, nulls, resolver))
    return exp.Order(expressions=ordered)


def _ordered_key(index, key, ascending, nulls, resolver) -> exp.Ordered:
    """Build one ordered key, honoring its ascending flag and NULLS placement."""
    desc = bool(ascending) and index < len(ascending) and not ascending[index]
    spec = nulls[index] if nulls and index < len(nulls) else None
    ast = expression_to_ast(key, resolver)
    if spec is None:
        return exp.Ordered(this=ast, desc=desc)
    return exp.Ordered(this=ast, desc=desc, nulls_first=(spec.upper() == "FIRST"))


def group_by(group_keys, grouping_sets, resolver):
    """Build an ``exp.Group`` for GROUP BY or GROUPING SETS, or None when absent."""
    if grouping_sets is not None:
        return _grouping_sets(grouping_sets, resolver)
    if not group_keys:
        return None
    keys = []
    for key in group_keys:
        keys.append(expression_to_ast(key, resolver))
    return exp.Group(expressions=keys)


def _grouping_sets(grouping_sets, resolver) -> exp.Group:
    """Build ``GROUPING SETS ((a, b), (a), ())`` from explicit grouping sets."""
    sets = []
    for grouping_set in grouping_sets:
        members = []
        for key in grouping_set:
            members.append(expression_to_ast(key, resolver))
        sets.append(exp.Tuple(expressions=members))
    return exp.Group(grouping_sets=[exp.GroupingSets(expressions=sets)])


def apply_limit_offset(select: exp.Select, limit, offset) -> exp.Select:
    """Attach LIMIT and/or OFFSET; OFFSET is valid and kept without a LIMIT."""
    if limit is not None:
        select = select.limit(limit)
    if offset:
        select = select.offset(offset)
    return select
