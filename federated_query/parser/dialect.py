"""Custom sqlglot Postgres dialect with native EXPLAIN parsing.

Postgres' stock sqlglot dialect tokenizes ``EXPLAIN`` as a generic
``COMMAND``, which forces the engine to re-parse the wrapped statement from
a string. Mapping ``EXPLAIN`` to the ``DESCRIBE`` token (the same mechanism
sqlglot's MySQL dialect uses) lets sqlglot build a real ``exp.Describe``
whose ``this`` is the already-parsed inner statement, so the engine never
re-scans SQL text.
"""

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class FedQPostgres(Postgres):
    """Postgres dialect that parses EXPLAIN into a native ``exp.Describe``."""

    class Tokenizer(Postgres.Tokenizer):
        """Tokenizer that classifies EXPLAIN as the DESCRIBE keyword."""

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "EXPLAIN": TokenType.DESCRIBE,
        }

    class Parser(Postgres.Parser):
        """Parser that consumes Postgres EXPLAIN options before the statement."""

        def _parse_describe(self) -> exp.Describe:
            """Parse ``EXPLAIN [options] <statement>`` into an ``exp.Describe``.

            ``as_json`` records whether the options asked for JSON output so
            the logical-plan builder can pick the EXPLAIN format directly.
            """
            requests_json = self._consume_explain_options()
            statement = self._parse_statement()
            return self.expression(exp.Describe(this=statement, as_json=requests_json))

        def _consume_explain_options(self) -> bool:
            """Consume the EXPLAIN option prefix; report a JSON request."""
            if self._match(TokenType.L_PAREN):
                return self._consume_paren_options()
            self._match_texts(("ANALYZE", "VERBOSE"))
            return False

        def _consume_paren_options(self) -> bool:
            """Consume a ``( option [, ...] )`` list and detect JSON output."""
            option_words = []
            while not self._match(TokenType.R_PAREN):
                option_words.append(self._consume_option_word())
            return self._options_request_json(option_words)

        def _consume_option_word(self) -> str:
            """Advance over one option token, returning its upper-cased text."""
            token = self._curr
            if token is None:
                raise ValueError("EXPLAIN options missing closing parenthesis")
            self._advance()
            return token.text.upper()

        def _options_request_json(self, option_words) -> bool:
            """Return True when ``FORMAT JSON`` or ``AS JSON`` is present."""
            for index in range(len(option_words) - 1):
                keyword = option_words[index]
                value = option_words[index + 1]
                if keyword in ("FORMAT", "AS") and value == "JSON":
                    return True
            return False

    class Generator(Postgres.Generator):
        """Generator that renders ``exp.Describe`` back as Postgres EXPLAIN."""

        def describe_sql(self, expression: exp.Describe) -> str:
            """Emit ``EXPLAIN [(FORMAT JSON)] <statement>`` for a Describe node."""
            options = ""
            if expression.args.get("as_json"):
                options = " (FORMAT JSON)"
            inner_sql = self.sql(expression, "this")
            return f"EXPLAIN{options} {inner_sql}"
