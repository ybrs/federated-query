"""Shared SQL-clause builders over the sqlglot AST.

One implementation each of the SELECT list, ORDER BY, GROUP BY / GROUPING SETS,
and LIMIT/OFFSET application. Every node that emits SQL - remote scans,
single-source pushdown, and the local DuckDB merge operators - composes these,
so clause rendering exists in exactly one place.
"""

from sqlglot import exp

from .expressions import expression_to_ast, ordered_key_from_ast
from ..logical import SetOpKind

# Engine set-operation kind -> sqlglot node; distinct=False renders ``... ALL``.
# The one mapping shared by the remote set-op node and single-source pushdown.
SET_OP_EXP = {
    SetOpKind.UNION: exp.Union,
    SetOpKind.INTERSECT: exp.Intersect,
    SetOpKind.EXCEPT: exp.Except,
}


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


def aliased_select_fragment(exprs, names, render_one) -> str:
    """Render each expression to ``<sql> AS "name"`` and join with ', '.

    The render-and-join skeleton every SELECT-list builder repeats, with the one
    genuine difference - how a single expression renders to SQL - supplied by
    ``render_one(expr) -> str``. An expression with no parallel name is emitted
    without an alias.
    """
    parts = []
    for index, expr in enumerate(exprs):
        fragment = render_one(expr)
        name = names[index] if names and index < len(names) else None
        if name:
            fragment = f'{fragment} AS "{name}"'
        parts.append(fragment)
    return ", ".join(parts)


def select_expressions_fragment(exprs, names, resolver, dialect="postgres") -> str:
    """Render aliased SELECT items to a comma-separated SQL fragment.

    The expression renderer is the one emitter (expression_to_ast) for the given
    resolver and dialect; the alias-and-join skeleton is aliased_select_fragment,
    shared with the aggregate SELECT list.
    """
    return aliased_select_fragment(
        exprs, names, lambda expr: expression_to_ast(expr, resolver).sql(dialect=dialect)
    )


def order_by(keys, ascending, nulls, resolver):
    """Build an ``exp.Order`` from parallel key/ascending/nulls lists, or None.

    Every key gets an explicit NULLS placement (the plan's, or the canonical
    Postgres default - LAST for ASC, FIRST for DESC), so NULL ordering is
    consistent across sources and never left to a dialect's differing default.
    """
    if not keys:
        return None
    ordered = []
    for index, key in enumerate(keys):
        ordered.append(_ordered_key(index, key, ascending, nulls, resolver))
    return exp.Order(expressions=ordered)


def _ordered_key(index, key, ascending, nulls, resolver) -> exp.Ordered:
    """Build one ORDER BY key; shares the NULLS-default rule with the emitter."""
    return ordered_key_from_ast(
        expression_to_ast(key, resolver), index, ascending, nulls
    )


def order_by_fragment(keys, ascending, nulls, resolver, dialect="postgres"):
    """Render ORDER BY keys to a comma-separated SQL fragment (no ``ORDER BY``).

    The single ``order_by`` builder produces the AST; each key is then rendered
    to ``dialect``. Used by every node that assembles ORDER BY into a SQL string
    (remote scans/joins/pushdown and the local merge operators).
    """
    order = order_by(keys, ascending, nulls, resolver)
    parts = []
    for item in order.expressions:
        parts.append(item.sql(dialect=dialect))
    return ", ".join(parts)


def group_by_fragment(group_keys, grouping_sets, resolver, dialect="postgres"):
    """Render GROUP BY keys / GROUPING SETS to a SQL fragment (no ``GROUP BY``).

    Returns None when there is nothing to group by.
    """
    group = group_by(group_keys, grouping_sets, resolver)
    if group is None:
        return None
    text = group.sql(dialect=dialect)
    prefix = "GROUP BY "
    return text[len(prefix) :] if text.startswith(prefix) else text


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


def apply_distinct(select: exp.Select, distinct, distinct_on) -> exp.Select:
    """Attach DISTINCT or DISTINCT ON (keys) to a SELECT.

    ``distinct_on`` is a list of already-built key ASTs; a bare ``distinct``
    flag is a plain DISTINCT. DISTINCT ON wins when both are set.
    """
    if distinct_on:
        select.set("distinct", exp.Distinct(on=exp.Tuple(expressions=list(distinct_on))))
    elif distinct:
        select.set("distinct", exp.Distinct())
    return select


def assemble_select(
    from_clause: exp.Expression,
    select_items,
    joins=None,
    where=None,
    group=None,
    having=None,
    distinct=False,
    distinct_on=None,
    order=None,
    limit=None,
    offset=0,
) -> exp.Select:
    """Compose a SELECT from already-built AST pieces - the one skeleton builder.

    Every part is a sqlglot node (or None): ``from_clause`` the base relation,
    ``joins`` its join chain (exp.Join nodes), ``select_items`` aliased projection
    items, ``where``/``having`` predicate ASTs, ``group``/``order`` exp.Group /
    exp.Order. Both the remote single-table scan (no joins) and the same-source
    N-table subtree build their pieces and assemble here, so the N=1 and N>1
    cases of one query share one structure.
    """
    select = exp.Select(expressions=list(select_items)).from_(from_clause)
    if joins:
        select.set("joins", list(joins))
    if where is not None:
        select.set("where", exp.Where(this=where))
    if group is not None:
        select.set("group", group)
    if having is not None:
        select.set("having", exp.Having(this=having))
    select = apply_distinct(select, distinct, distinct_on)
    if order is not None:
        select.set("order", order)
    return apply_limit_offset(select, limit, offset)
