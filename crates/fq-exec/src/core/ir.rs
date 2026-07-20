// Imported fedqrs engine core - already validated by the TPC tallies; pedantic-
// cleaned incrementally, not on import.
#![allow(clippy::all, clippy::pedantic)]
//! The serializable execution IR.
//!
//! Python builds this from its physical plan and hands it over as JSON. Two
//! layers:
//!
//!   1. Orchestration steps (`Step`) - an ordered list. Source scans are read
//!      natively in Rust; the one thing relational algebra cannot express is the
//!      runtime feedback edge where a probe scan is emitted only after the
//!      build side's distinct keys are known (semi-join reduction). The keys are
//!      computed and injected entirely in Rust; nothing crosses back to Python.
//!
//!   2. Relational fragments (`Fragment`) - the local operators run on
//!      DataFusion. Each fragment fully specifies one operator.
//!
//! Both layers share one expression sub-IR (`IrExpr`), which drives both source
//! SQL emission (via DataFusion's unparser) and local operator construction.
//!
//! Every step writes a named `binding`; later steps read bindings by name.

use std::collections::BTreeMap;

use serde::Deserialize;

/// The whole plan: an ordered step list plus the fragment definitions the
/// `Merge` steps refer to by name.
#[derive(Debug, Deserialize)]
pub struct Ir {
    #[serde(default)]
    pub outputs: Vec<String>,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub fragments: BTreeMap<String, Fragment>,
}

/// One orchestration step. Tagged by `op` in JSON.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Step {
    /// Read a source table natively and bind its Arrow result. `materialize`
    /// keeps the whole result in memory (needed when a binding is scanned more
    /// than once, e.g. a hash-join build side that also feeds `collect_distinct`).
    SourceScan {
        datasource: String,
        scan: ScanSpec,
        binding: String,
        #[serde(default)]
        materialize: bool,
    },

    /// Compute the DISTINCT, NULL-free values of one key column of a
    /// materialized binding, capped at `cap`. Binds a small keys table, or a
    /// marker that the count exceeded the cap (so no dynamic filter is pushed).
    CollectDistinct {
        input: String,
        key: String,
        cap: usize,
        binding: String,
    },

    /// Read a probe source with the collected build keys pushed into its SQL as
    /// `inject_column IN (...)`. The IN list is built in Rust from the
    /// `keys_from` binding; if that binding is over cap, the scan runs in full.
    InjectedScan {
        datasource: String,
        scan: ScanSpec,
        inject_column: String,
        keys_from: String,
        binding: String,
        /// The probe column's source-catalog NDV, threaded from the planner's
        /// statistics cache. The delivery-strategy guard estimates the
        /// fetched fraction as keys/NDV; without it the source-side fallback
        /// (keys/row-count on DuckDB) UNDERESTIMATES selectivity for
        /// near-superset key sets.
        #[serde(default)]
        inject_column_ndv: Option<u64>,
        /// Further reductions ANDed onto the same read as bounded IN lists
        /// (one scan can carry SEVERAL dimensions' keys - store AND date).
        /// The PRIMARY injection above keeps the full delivery-strategy
        /// machinery; an extra whose key set exceeds the IN cap is simply
        /// skipped - extras are a bonus, never a requirement.
        #[serde(default)]
        extra_injections: Vec<ExtraInjection>,
    },

    /// Materialize a binding as a TEMP TABLE on a target source, so a later
    /// scan on that source can join it natively (dim shipping: a small
    /// filtered dimension travels TO the fact's source and the whole
    /// join+aggregate collapses into one island there).
    Ship {
        datasource: String,
        input: String,
        table: String,
    },

    /// Run a relational fragment over named inputs and bind its result. `inputs`
    /// maps the fragment's input table name (e.g. `in_left`) to a binding.
    Merge {
        fragment: String,
        inputs: BTreeMap<String, String>,
        binding: String,
    },

    /// The binding whose Arrow stream is exported back to Python.
    Return { input: String },
}

/// A source scan. Either a structured single-table scan that Rust renders to
/// dialect SQL, or a pre-rendered `raw_sql` string (used for a complex
/// One additional key set ANDed onto an injected scan as a bounded IN list.
/// Clone: a prefetched injected scan ships an owned copy of its spec to a
/// worker thread (the job cannot borrow the IR across the pool boundary).
#[derive(Debug, Clone, Deserialize)]
pub struct ExtraInjection {
    pub column: String,
    pub keys_from: String,
}

/// single-source subtree whose SQL Python already emitted). A scan that a
/// dynamic filter is injected into must be structured, so Rust can add the
/// `IN (...)` predicate.
/// Clone: a prefetched injected scan ships an owned copy of its spec to a
/// worker thread (the job cannot borrow the IR across the pool boundary).
#[derive(Debug, Clone, Deserialize)]
pub struct ScanSpec {
    /// Pre-rendered SQL for the whole scan. When set, the structured fields are
    /// ignored and no dynamic filter may be injected.
    #[serde(default)]
    pub raw_sql: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
    /// Output columns, in order. Rendered as the SELECT list.
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub filter: Option<IrExpr>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub distinct: bool,
    /// Read this scan with the ctid-partitioned parallel Postgres path. Set by
    /// the planner only for big plain table reads (structured, no DISTINCT, no
    /// LIMIT); the engine validates and refuses anything else loudly.
    #[serde(default)]
    pub parallel: bool,
    /// For a raw-SQL probe of a key reduction: the island SQL prerendered by
    /// the planner with the key filter already ON ITS OWNING BASE RELATION,
    /// reading the keys from the `fedq_dyn_keys` temp table. Preferred over
    /// wrapping the island output, which sources cannot push down through.
    #[serde(default)]
    pub injected_sql: Option<String>,
}

/// A single local relational operator. Tagged by `kind` in JSON.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fragment {
    /// An equi-join of two inputs (`in_left`, `in_right`) on paired key columns.
    /// `project` produces the join's canonical output columns so a parent
    /// fragment can reference them; a user SELECT list is a separate `Project`.
    HashJoin {
        join_type: JoinKind,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        project: Vec<Projection>,
    },
    /// A non-equi (nested-loop) join of two inputs (`in_left`, `in_right`) on an
    /// arbitrary boolean condition (`None` = cross join). `project` is the
    /// canonical output column list, like `HashJoin`.
    NestedLoopJoin {
        join_type: JoinKind,
        #[serde(default)]
        condition: Option<IrExpr>,
        project: Vec<Projection>,
    },
    /// A projection over a single input (`in_0`): evaluate each expression and
    /// alias it to the output column name. `distinct` deduplicates the
    /// projected rows (SELECT DISTINCT).
    Project {
        project: Vec<Projection>,
        #[serde(default)]
        distinct: bool,
    },
    /// A GROUP BY (or grand-total) aggregation over a single input (`in_0`).
    /// `select` is the output list (aggregate calls and grouping expressions);
    /// `group_by` is the grouping key list.
    Aggregate {
        select: Vec<AggSelectItem>,
        #[serde(default)]
        group_by: Vec<IrExpr>,
        /// When non-empty, the GROUP BY is `GROUPING SETS (...)`; each inner list
        /// is one grouping set (ROLLUP/CUBE are pre-expanded to sets).
        #[serde(default)]
        grouping_sets: Vec<Vec<IrExpr>>,
    },
    /// An ORDER BY over a single input (`in_0`).
    Sort { keys: Vec<SortKey> },
    /// A boolean filter over a single input (`in_0`).
    Filter { predicate: IrExpr },
    /// A LIMIT/OFFSET over a single input (`in_0`). `limit` is None for OFFSET
    /// with no row cap.
    Limit {
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        offset: usize,
    },
    /// The scalar-subquery cardinality guard over a single input (`in_0`).
    /// With no keys the input may hold at most ONE row in total; with keys, at
    /// most one row per distinct key tuple (the decorrelated per-outer-row
    /// rule). A violation is an execution error - a scalar subquery must be
    /// single-valued, so silently joining a multi-row side would duplicate
    /// outer rows. Otherwise the input passes through unchanged.
    SingleRowGuard {
        #[serde(default)]
        keys: Vec<IrExpr>,
    },
    /// Run a pre-rendered SQL statement over the merge inputs (registered under
    /// their names). The escape hatch for a whole WITH / CTE that Python already
    /// rendered; DataFusion parses and executes it.
    RawSql { sql: String },
}

/// One ORDER BY key: an expression with sort direction and NULL placement.
#[derive(Debug, Deserialize)]
pub struct SortKey {
    pub expr: IrExpr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// One output column of an aggregate: exactly one of `expr` (a plain grouping
/// expression) or `agg` (an aggregate call), aliased to the output name.
#[derive(Debug, Deserialize)]
pub struct AggSelectItem {
    #[serde(default)]
    pub expr: Option<IrExpr>,
    #[serde(default)]
    pub agg: Option<AggCall>,
    pub alias: String,
}

/// An aggregate function call, e.g. `count(*)`, `sum(x)`, `count(DISTINCT y)`,
/// or an ordered-set aggregate `percentile_cont(0.5) WITHIN GROUP (ORDER BY y)`.
#[derive(Debug, Deserialize)]
pub struct AggCall {
    pub func: String,
    #[serde(default)]
    pub distinct: bool,
    /// `count(*)` — no argument, counts rows.
    #[serde(default)]
    pub star: bool,
    #[serde(default)]
    pub args: Vec<IrExpr>,
    /// Present for an ordered-set aggregate: the `WITHIN GROUP (ORDER BY ...)`.
    #[serde(default)]
    pub within_group: Option<WithinGroup>,
}

/// The `WITHIN GROUP (ORDER BY key [DESC])` of an ordered-set aggregate.
#[derive(Debug, Deserialize)]
pub struct WithinGroup {
    pub key: IrExpr,
    #[serde(default)]
    pub desc: bool,
}

/// An output column of a fragment: an expression aliased to a result name.
#[derive(Debug, Deserialize)]
pub struct Projection {
    pub expr: IrExpr,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// The expression sub-IR. Tagged by `node` in JSON. Column references are
/// already resolved to their physical relation/column names by Python.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum IrExpr {
    Column {
        #[serde(default)]
        relation: Option<String>,
        name: String,
    },
    Literal {
        value: LiteralValue,
    },
    Binary {
        op: String,
        left: Box<IrExpr>,
        right: Box<IrExpr>,
    },
    Unary {
        op: String,
        operand: Box<IrExpr>,
    },
    Cast {
        expr: Box<IrExpr>,
        /// Arrow type name, e.g. "int64", "utf8", "float64", "boolean".
        to: String,
    },
    Case {
        #[serde(default)]
        operand: Option<Box<IrExpr>>,
        whens: Vec<WhenThen>,
        #[serde(default, rename = "else")]
        else_expr: Option<Box<IrExpr>>,
    },
    InList {
        expr: Box<IrExpr>,
        list: Vec<IrExpr>,
        #[serde(default)]
        negated: bool,
    },
    IsNull {
        expr: Box<IrExpr>,
        #[serde(default)]
        negated: bool,
    },
    /// `expr [I]LIKE pattern [ESCAPE 'c']`. `escape` is the single escape
    /// character; `case_insensitive` selects ILIKE. Negation is carried by a
    /// wrapping `Unary { op: "not" }`, so this node is never negated on its own.
    Like {
        expr: Box<IrExpr>,
        pattern: Box<IrExpr>,
        #[serde(default)]
        case_insensitive: bool,
        #[serde(default)]
        escape: Option<char>,
    },
    /// A scalar function call resolved by name against DataFusion's built-in
    /// functions (e.g. `date_part`, `substr`). EXTRACT lowers to `date_part`.
    Function {
        name: String,
        #[serde(default)]
        args: Vec<IrExpr>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct WhenThen {
    pub when: IrExpr,
    pub then: IrExpr,
}

/// A typed literal value.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "lit", rename_all = "snake_case")]
pub enum LiteralValue {
    Int { value: i64 },
    Float { value: f64 },
    Str { value: String },
    Bool { value: bool },
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_step_and_fragment_tags() {
        let ir: Ir = serde_json::from_str(
            r#"{"outputs":["x"],"steps":[
                 {"op":"source_scan","datasource":"pg","scan":{"raw_sql":"SELECT 1"},"binding":"b1"},
                 {"op":"collect_distinct","input":"b1","key":"k","cap":2000,"binding":"b2"},
                 {"op":"injected_scan","datasource":"pg","scan":{"table":"t","columns":["k"]},
                  "inject_column":"k","keys_from":"b2","binding":"b3"},
                 {"op":"merge","fragment":"f","inputs":{"in_0":"b3"},"binding":"b4"},
                 {"op":"return","input":"b4"}],
               "fragments":{"f":{"kind":"aggregate","select":[
                  {"agg":{"func":"COUNT","distinct":false,"star":true,"args":[]},"alias":"n"}],
                  "group_by":[]}}}"#,
        )
        .unwrap();
        assert_eq!(ir.steps.len(), 5);
        assert!(matches!(ir.steps[0], Step::SourceScan { .. }));
        assert!(matches!(ir.steps[2], Step::InjectedScan { .. }));
        assert!(matches!(ir.steps[4], Step::Return { .. }));
        assert!(matches!(
            ir.fragments.get("f"),
            Some(Fragment::Aggregate { .. })
        ));
    }

    #[test]
    fn parses_single_row_guard_fragment() {
        // Keyed form carries key expressions; the keyless form (empty or
        // absent `keys`, via serde default) is the at-most-one-row-total guard.
        let keyed: Fragment = serde_json::from_str(
            r#"{"kind":"single_row_guard","keys":[
                {"node":"column","relation":"in_0","name":"k0"}]}"#,
        )
        .unwrap();
        assert!(matches!(keyed, Fragment::SingleRowGuard { ref keys } if keys.len() == 1));
        let keyless: Fragment = serde_json::from_str(r#"{"kind":"single_row_guard"}"#).unwrap();
        assert!(matches!(keyless, Fragment::SingleRowGuard { ref keys } if keys.is_empty()));
    }

    #[test]
    fn scanspec_defaults_apply() {
        // Only table+columns given; optional fields default.
        let s: ScanSpec = serde_json::from_str(r#"{"table":"t","columns":["a"]}"#).unwrap();
        assert_eq!(s.table.as_deref(), Some("t"));
        assert!(s.raw_sql.is_none() && s.filter.is_none() && !s.distinct);
    }

    #[test]
    fn rejects_unknown_op() {
        assert!(serde_json::from_str::<Ir>(
            r#"{"steps":[{"op":"teleport","binding":"b"}],"fragments":{}}"#
        )
        .is_err());
    }
}
