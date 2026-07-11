//! The step / fragment / scan-spec plain structs `build_steps` produces and
//! `fq-exec` later consumes. Ports the SHAPE of `fedqrs/core/src/ir.rs` (Step,
//! ScanSpec, ExtraInjection, Fragment, SortKey, AggSelectItem, AggCall,
//! WithinGroup, Projection, JoinKind) with two mechanical edits: every `IrExpr`
//! field becomes `fq_plan::Expr` (held directly, retagged in place, NOT
//! serialized), and every serde derive is dropped - the enum IS the tag.

use std::collections::BTreeMap;

use fq_plan::Expr;

/// One orchestration step the engine runs in order. (ir.rs `Step`, no `op` tag.)
#[derive(Debug, Clone)]
pub enum Step {
    /// Read a source relation natively (structured+parallel when it qualifies, else
    /// pre-rendered raw SQL). `materialize` forces a fully collected binding.
    SourceScan {
        datasource: String,
        scan: ScanSpec,
        binding: String,
        materialize: bool,
    },
    /// Collect a build side's distinct join-key values, capped, for injection.
    CollectDistinct {
        input: String,
        key: String,
        cap: usize,
        binding: String,
    },
    /// A probe base with the build's keys pushed in as `col IN (...)`, plus any
    /// bounded extra IN-lists ANDed on.
    InjectedScan {
        datasource: String,
        scan: ScanSpec,
        inject_column: String,
        keys_from: String,
        binding: String,
        inject_column_ndv: Option<i64>,
        extra_injections: Vec<ExtraInjection>,
    },
    /// Materialize a foreign relation as a temp table on `datasource` (dim ship).
    Ship {
        datasource: String,
        input: String,
        table: String,
    },
    /// Run a local relational fragment over named inputs.
    Merge {
        fragment: String,
        inputs: BTreeMap<String, String>,
        binding: String,
    },
    /// The final result binding.
    Return { input: String },
}

impl Step {
    /// The output binding a producing step holds; `Return`/`Ship` produce none.
    pub fn binding(&self) -> Option<&str> {
        match self {
            Step::SourceScan { binding, .. }
            | Step::CollectDistinct { binding, .. }
            | Step::InjectedScan { binding, .. }
            | Step::Merge { binding, .. } => Some(binding),
            Step::Ship { .. } | Step::Return { .. } => None,
        }
    }
}

/// One extra IN-list ANDed onto an injected scan. (ir.rs `ExtraInjection`.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtraInjection {
    pub column: String,
    pub keys_from: String,
}

/// A source scan: a structured single-table read, or pre-rendered `raw_sql`.
/// `filter` is the source-side scan filter - its columns keep their OWN qualifier,
/// NOT retagged (fq-exec renders it against the source). (ir.rs `ScanSpec`.)
#[derive(Debug, Clone)]
pub struct ScanSpec {
    pub raw_sql: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub alias: Option<String>,
    pub columns: Vec<String>,
    pub filter: Option<Expr>,
    pub limit: Option<u64>,
    pub distinct: bool,
    pub parallel: bool,
    pub injected_sql: Option<String>,
}

impl ScanSpec {
    /// An empty structured scan spec (no raw SQL, no columns) to fill in.
    pub fn empty() -> Self {
        Self {
            raw_sql: None,
            schema: None,
            table: None,
            alias: None,
            columns: Vec::new(),
            filter: None,
            limit: None,
            distinct: false,
            parallel: false,
            injected_sql: None,
        }
    }

    /// A raw-SQL scan spec carrying a pre-rendered source query.
    pub fn raw(sql: String) -> Self {
        Self {
            raw_sql: Some(sql),
            ..Self::empty()
        }
    }
}

/// A single local relational operator. (ir.rs `Fragment`, no `kind` tag.)
#[derive(Debug, Clone)]
pub enum Fragment {
    HashJoin {
        join_type: JoinKind,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        project: Vec<Projection>,
    },
    NestedLoopJoin {
        join_type: JoinKind,
        condition: Option<Expr>,
        project: Vec<Projection>,
    },
    Project {
        project: Vec<Projection>,
        distinct: bool,
    },
    Aggregate {
        select: Vec<AggSelectItem>,
        group_by: Vec<Expr>,
        grouping_sets: Vec<Vec<Expr>>,
    },
    Sort {
        keys: Vec<SortKey>,
    },
    Filter {
        predicate: Expr,
    },
    Limit {
        limit: Option<u64>,
        offset: u64,
    },
    SingleRowGuard {
        keys: Vec<Expr>,
    },
    RawSql {
        sql: String,
    },
}

/// One ORDER BY key of a sort fragment. (ir.rs `SortKey`.)
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// One aggregate-select output: exactly one of `expr` / `agg` set, aliased.
/// (ir.rs `AggSelectItem`.)
#[derive(Debug, Clone)]
pub struct AggSelectItem {
    pub expr: Option<Expr>,
    pub agg: Option<AggCall>,
    pub alias: String,
}

/// An aggregate function call in an aggregate fragment. (ir.rs `AggCall`.)
#[derive(Debug, Clone)]
pub struct AggCall {
    pub func: String,
    pub distinct: bool,
    pub star: bool,
    pub args: Vec<Expr>,
    pub within_group: Option<WithinGroup>,
}

/// An ordered-set aggregate's WITHIN GROUP sort key. (ir.rs `WithinGroup`.)
#[derive(Debug, Clone)]
pub struct WithinGroup {
    pub key: Expr,
    pub desc: bool,
}

/// One projection item: a retagged expression aliased to an output name.
/// (ir.rs `Projection`.)
#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: Expr,
    pub alias: String,
}

/// A join kind the merge engine understands. (ir.rs `JoinKind`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// What a binding's engine-measured output row count MEANS for the learned-stats
/// catalog: the provenance side-channel kept OUT of the steps (a write-path
/// concern). Ports the Python observation dicts. (rust_ir `ctx.observations`.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// The binding's rows are a base table's row count.
    TableRows {
        datasource: String,
        schema: String,
        table: String,
    },
    /// The binding's rows are a base column's NDV (a collect_distinct output).
    ColumnNdv {
        datasource: String,
        schema: String,
        table: String,
        column: String,
    },
    /// The binding's rows are a GROUP BY's group count for `subject`/`columns`.
    Group {
        subject: String,
        columns: Vec<String>,
    },
    /// The binding's rows are a filter template's measured output.
    Predicate {
        datasource: String,
        schema: String,
        table: String,
        template: String,
    },
}

/// The `build_steps` result bundle: the ordered steps, the fragment map, the
/// result column names, and the observation provenance. Ports the (ir, observations)
/// pair of `build_ir_with_observations`.
#[derive(Debug, Clone)]
pub struct BuiltSteps {
    pub outputs: Vec<String>,
    pub steps: Vec<Step>,
    pub fragments: BTreeMap<String, Fragment>,
    pub observations: BTreeMap<String, Observation>,
}
