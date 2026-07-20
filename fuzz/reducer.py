"""Greedy minimizer for mutation-derived findings.

Hypothesis shrinks strategy-generated findings on its own; mutation-derived
findings start from a corpus query plus a text edit, so this reducer shrinks them
greedily: it repeatedly tries a set of structure-dropping edits (unwrap a derived
table, drop a WHERE conjunct, a projection item, a join, or an ORDER BY/LIMIT/
HAVING clause) and keeps any edit that still reproduces the failure, until no edit
helps. Each candidate is validated by a caller-supplied predicate that re-runs the
exact failing check and reports whether the same signature still fires, so an edit
that produces invalid or differently-behaving SQL is simply rejected.

The reducer is text-based and paren/quote aware so it edits only top-level
clauses and never disturbs the ``{table_name}`` placeholders.
"""

_BOUNDARY_KEYWORDS = (
    " FROM ",
    " WHERE ",
    " GROUP BY ",
    " HAVING ",
    " ORDER BY ",
    " LIMIT ",
    " UNION ",
    " INTERSECT ",
    " EXCEPT ",
)


def _top_positions(sql, keyword):
    """Return indices where ``keyword`` appears at paren depth 0, outside quotes."""
    upper = sql.upper()
    target = keyword.upper()
    positions = []
    depth = 0
    in_quote = False
    for index in range(len(sql)):
        depth, in_quote = _scan_step(sql[index], depth, in_quote)
        if depth == 0 and not in_quote and upper.startswith(target, index):
            positions.append(index)
    return positions


def _scan_step(char, depth, in_quote):
    """Advance the paren-depth / in-quote scanner by one character."""
    if char == "'":
        return depth, not in_quote
    if not in_quote and char == "(":
        return depth + 1, in_quote
    if not in_quote and char == ")":
        return depth - 1, in_quote
    return depth, in_quote


def _first_top(sql, keyword):
    """Return the first top-level index of ``keyword`` or -1."""
    positions = _top_positions(sql, keyword)
    if positions:
        return positions[0]
    return -1


def _clause_extent(sql, keyword):
    """Return (start, end) of a top-level clause, ending at the next boundary."""
    start = _first_top(sql, keyword)
    if start < 0:
        return None
    end = _next_boundary(sql, start + len(keyword))
    return start, end


def _next_boundary(sql, after):
    """Return the earliest top-level boundary keyword index at or after ``after``."""
    best = len(sql)
    for keyword in _BOUNDARY_KEYWORDS:
        for position in _top_positions(sql, keyword):
            if position >= after and position < best:
                best = position
    return best


def unwrap_derived(sql):
    """Reduce ``SELECT ... FROM (<inner>) mm ...`` to its inner query, or None."""
    from_index = _first_top(sql, " FROM ")
    if from_index < 0:
        return None
    open_index = _first_nonspace(sql, from_index + len(" FROM "))
    if open_index is None or sql[open_index] != "(":
        return None
    close_index = _matching_paren(sql, open_index)
    if close_index is None:
        return None
    return sql[open_index + 1 : close_index].strip()


def _first_nonspace(sql, start):
    """Return the index of the first non-space character at or after start."""
    for index in range(start, len(sql)):
        if sql[index] != " ":
            return index
    return None


def _matching_paren(sql, open_index):
    """Return the index of the parenthesis matching the one at ``open_index``."""
    depth = 0
    in_quote = False
    for index in range(open_index, len(sql)):
        depth, in_quote = _scan_step(sql[index], depth, in_quote)
        if depth == 0 and not in_quote:
            return index
    return None


def drop_where_conjunct(sql):
    """Drop the last top-level ``AND`` conjunct of the WHERE clause, or the WHERE."""
    extent = _clause_extent(sql, " WHERE ")
    if extent is None:
        return None
    start, end = extent
    body = sql[start + len(" WHERE ") : end]
    conjuncts = _split_top(body, " AND ")
    if len(conjuncts) > 1:
        kept = " AND ".join(conjuncts[:-1])
        return sql[:start] + " WHERE " + kept + sql[end:]
    return sql[:start] + sql[end:]


def _split_top(text, separator):
    """Split ``text`` on a separator that appears at paren depth 0, outside quotes."""
    positions = _top_positions(text, separator)
    if not positions:
        return [text]
    return _slice_at(text, positions, len(separator))


def _slice_at(text, positions, width):
    """Slice ``text`` into the segments delimited by the given positions."""
    parts = []
    previous = 0
    for position in positions:
        parts.append(text[previous:position])
        previous = position + width
    parts.append(text[previous:])
    return parts


def drop_projection_item(sql):
    """Drop the last top-level projection item (keeping at least one), or None."""
    from_index = _first_top(sql, " FROM ")
    if from_index < 0 or not sql.upper().startswith("SELECT "):
        return None
    body = sql[len("SELECT ") : from_index]
    items = _split_top(body, ",")
    if len(items) < 2:
        return None
    kept = ",".join(items[:-1])
    return "SELECT " + kept + sql[from_index:]


def drop_clause(sql, keyword):
    """Drop a whole top-level clause named by ``keyword`` (ORDER BY/LIMIT/HAVING)."""
    extent = _clause_extent(sql, keyword)
    if extent is None:
        return None
    start, end = extent
    return sql[:start] + sql[end:]


def drop_last_join(sql):
    """Drop the last top-level ``JOIN ... ON ...`` segment, or None if none."""
    positions = _top_positions(sql, " JOIN ")
    if not positions:
        return None
    start = positions[-1]
    start = _join_prefix(sql, start)
    end = _next_boundary(sql, start + len(" JOIN "))
    return sql[:start] + sql[end:]


def _join_prefix(sql, join_index):
    """Extend a JOIN start left over a LEFT/RIGHT/FULL/INNER qualifier if present."""
    for qualifier in (" LEFT", " RIGHT", " FULL", " INNER"):
        if sql.upper().startswith(
            qualifier.upper() + " JOIN ", join_index - len(qualifier)
        ):
            return join_index - len(qualifier)
    return join_index


_TRANSFORMS = (
    unwrap_derived,
    drop_last_join,
    drop_where_conjunct,
    drop_projection_item,
    lambda sql: drop_clause(sql, " ORDER BY "),
    lambda sql: drop_clause(sql, " LIMIT "),
    lambda sql: drop_clause(sql, " HAVING "),
)


def reduce_query(sql, predicate, max_rounds=40):
    """Greedily shrink ``sql`` while ``predicate`` still reproduces the failure.

    ``predicate(candidate_sql)`` returns True when the candidate still triggers
    the same finding. Returns the smallest reproducing SQL found (the input itself
    if nothing reduces).
    """
    current = sql
    for _round in range(max_rounds):
        reduced = _one_round(current, predicate)
        if reduced is None:
            return current
        current = reduced
    return current


def _one_round(sql, predicate):
    """Apply the first transform that yields a smaller reproducing query, or None."""
    for transform in _TRANSFORMS:
        candidate = _try_transform(transform, sql, predicate)
        if candidate is not None:
            return candidate
    return None


def _try_transform(transform, sql, predicate):
    """Return a transform's candidate if it shrinks and still reproduces, else None."""
    candidate = transform(sql)
    if candidate is None or candidate == sql or len(candidate) >= len(sql):
        return None
    if predicate(candidate):
        return candidate
    return None
