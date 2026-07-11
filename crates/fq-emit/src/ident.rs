//! Identifier quoting for the canonical Postgres form.

/// Double-quote an identifier for Postgres/DuckDB, doubling embedded quotes so
/// the source sees exactly the intended name with no case-folding. Same rule as
/// the fedqrs `core/src/sql.rs::quote_ident`.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::quote_ident;

    #[test]
    fn quotes_a_plain_name() {
        assert_eq!(quote_ident("col"), "\"col\"");
    }

    #[test]
    fn doubles_an_embedded_quote() {
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }
}
