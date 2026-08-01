"""The pushed fragments a source answers differently from the oracle, and why.

An entry names one fragment the engine sends whose source disagrees with the
DuckDB reference over the same data. The fragment is still SENT and still
CHECKED - it is recorded, not excused - so the disagreement stays exercised and
stays visible instead of disappearing from the run.

Every entry is STRICT, exactly as ``known_failures`` is: a registered fragment
that starts agreeing with the oracle FAILS the run, which forces the entry to be
deleted in the change that fixes it.

An entry is only legitimate when the disagreement is a decision nobody has taken
yet rather than a defect nobody has fixed. A fragment that is simply wrong
belongs in the engine's fix list, not here.

The key is (source kind, canonical SQL): the same text sent to the same kind of
source is the same disagreement, whichever instance it happened to be read from.
"""

# A Parquet source's SQL is executed by DataFusion, which returns AVG over a
# decimal column as a decimal truncated to six fractional digits, where the
# reference keeps full double precision. The engine has not decided which
# arithmetic it implements, so both answers are defensible and neither is the
# engine's own; that decision is what this entry waits on.
_PARQUET_DECIMAL_AVG = (
    "a Parquet source computes AVG over a decimal column to six fractional "
    "digits where the DuckDB reference keeps double precision"
)

_AVG_PRICE_BY_CUSTOMER = (
    'SELECT AVG("o"."price") AS "__subq_0_v0", '
    '"o"."customer_id" AS "__subq_0_g0" '
    'FROM "main"."orders" AS "o" GROUP BY "o"."customer_id"'
)


def _entries():
    """Return (source kind, canonical SQL) -> reason for every known divergence."""
    known = {}
    known[("pq", _AVG_PRICE_BY_CUSTOMER)] = _PARQUET_DECIMAL_AVG
    return known


KNOWN_DIVERGENCES = _entries()


def lookup(source_kind, canonical_sql):
    """Return the reason this fragment is a known divergence, or None."""
    return KNOWN_DIVERGENCES.get((source_kind, canonical_sql))
