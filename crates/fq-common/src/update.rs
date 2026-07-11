//! `update!` - the one blessed copy-and-mutate for state nodes.
//!
//! Transforming a plan / expression node must NEVER re-list every field into a
//! fresh struct literal: on any change you are forced to retype every field, and
//! retyping the wrong value (a reset estimate, a dropped column list) compiles
//! clean and ships a wrong answer. `update!` takes an OWNED node, overwrites only
//! the named fields, and returns it - every other field is preserved untouched.
//! It is the Rust analogue of pydantic `model_copy(update=...)`.
//!
//! It is also cheap: it moves the node into a local and mutates in place, so for
//! an inline enum variant the compiler routinely keeps it in one slot (no heap
//! allocation - only `.clone()` of a subtree allocates). The one mutation idiom;
//! see `doc/node-mutation-and-concurrency.md` for the concurrency rationale
//! (owned + `Send` beats shared + `Arc` at the query/execution granularities that
//! matter, so in-place is thread-ready, not a corner we paint ourselves into).
//!
//! Use it for setting fields to fresh values. When a new value is DERIVED from
//! the old node, bind it yourself and mutate (`let mut n = node; n.filters =
//! combine(n.filters, extra); n`) - the val expressions here cannot reference the
//! node being moved. A genuinely NEW node (the parser building from SQL, or a
//! changed enum variant) is a plain construction, justified per the fq-lint rule.

/// Copy-and-mutate an owned node: overwrite only the named fields, preserve the
/// rest. `update!(scan, { limit = Some(10), offset = 0 })`.
#[macro_export]
macro_rules! update {
    ($node:expr, { $($field:ident = $val:expr),+ $(,)? }) => {{
        let mut updated = $node;
        $( updated.$field = $val; )+
        updated
    }};
}

#[cfg(test)]
mod tests {
    #[derive(Debug, PartialEq, Clone)]
    struct Node {
        a: i32,
        b: String,
        c: Vec<i32>,
    }

    #[test]
    fn overwrites_only_named_fields_and_preserves_the_rest() {
        let base = Node {
            a: 1,
            b: "keep".to_string(),
            c: vec![1, 2, 3],
        };
        let out = update!(base, { a = 9 });
        // a changed; b and c preserved untouched (the model_copy guarantee).
        assert_eq!(
            out,
            Node {
                a: 9,
                b: "keep".to_string(),
                c: vec![1, 2, 3],
            }
        );
    }

    #[test]
    fn overwrites_multiple_fields() {
        let base = Node {
            a: 1,
            b: "x".to_string(),
            c: vec![],
        };
        let out = update!(base, { a = 2, b = "y".to_string(), });
        assert_eq!(out.a, 2);
        assert_eq!(out.b, "y");
        assert!(out.c.is_empty());
    }
}
