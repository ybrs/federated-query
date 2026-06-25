"""Parser-related error types."""


class UnsupportedSQLError(Exception):
    """Raised when a SQL clause is parsed but the engine cannot plan it.

    Failing fast here keeps the engine from silently dropping a clause and
    returning a wrong answer (for example DISTINCT ON, named WINDOW, GROUP BY
    ROLLUP/CUBE/GROUPING SETS, TABLESAMPLE, or FETCH FIRST ... WITH TIES).
    """
