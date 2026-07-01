"""Parser-related error types."""


class UnsupportedSQLError(ValueError):
    """Raised when a SQL clause is parsed but the engine cannot plan it.

    Failing fast here keeps the engine from silently dropping a clause and
    returning a wrong answer (for example a named WINDOW clause, UNPIVOT,
    FETCH FIRST ... WITH TIES / PERCENT, TRY_CAST, or BETWEEN SYMMETRIC).

    Subclasses ValueError because an unsupported clause is a kind of invalid
    input; callers that catch ValueError (and existing rejection tests) keep
    working while the specific type stays available for targeted handling.
    """
