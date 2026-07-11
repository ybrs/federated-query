//! Step common-subexpression elimination: identical producing steps run once and
//! share a binding. Ports `_emit_step_once`, `_step_cache_key`, `_scan_share_key`,
//! `_canonical_scan_sql`, `_widen_cached_columns`, `_narrow_cached_extras`.
//!
//! CORRECTNESS-CRITICAL: a wrong share key merges two different scans = wrong rows.
//! The scan filter is canonicalized into the key via `fq_emit::render_canonical` (a
//! stable SQL string), NEVER `Debug`/pointer: two structurally-equal filters MUST
//! produce the same key, two different filters MUST produce different keys.

use fq_emit::render_canonical;
use fq_plan::physical::PhysicalScan;
use fq_plan::{ColumnRef, Expr, PhysicalPlan};

use super::render_sql::render_scan_sql;
use super::scan_spec::injectable_scan;
use super::types::{ScanSpec, Step};
use super::Ctx;

/// Append a producing step unless an identical one already ran; return the binding
/// that holds the data either way. On a cache hit the cached step's columns are
/// widened and its extra injections narrowed (see the two mutators). Ports
/// `_emit_step_once`. `key` defaults to `step_cache_key`.
pub fn emit_step_once(ctx: &mut Ctx, step: Step, key: Option<String>) -> String {
    let key = key.unwrap_or_else(|| step_cache_key(&step));
    if let Some(&index) = ctx.step_cache.get(&key) {
        // Reconcile the shared read with this new consumer, then reuse its binding.
        let (columns, extras) = merge_inputs(&step);
        widen_cached_columns(&mut ctx.steps[index], &columns);
        narrow_cached_extras(&mut ctx.steps[index], extras.as_deref());
        return ctx.steps[index]
            .binding()
            .expect("a cached producing step has a binding")
            .to_string();
    }
    let binding = step
        .binding()
        .expect("a producing step has a binding")
        .to_string();
    ctx.steps.push(step);
    ctx.step_cache.insert(key, ctx.steps.len() - 1);
    binding
}

/// The new consumer's structured columns and extra injections, extracted before
/// the cached step is borrowed mutably (avoids overlapping borrows).
fn merge_inputs(step: &Step) -> (Vec<String>, Option<Vec<super::types::ExtraInjection>>) {
    match step {
        Step::SourceScan { scan, .. } => (scan.columns.clone(), None),
        Step::InjectedScan {
            scan,
            extra_injections,
            ..
        } => (scan.columns.clone(), Some(extra_injections.clone())),
        _ => (Vec::new(), None),
    }
}

/// A step's identity string: every field except its output binding, a structured
/// scan spec's mergeable columns, and mergeable extra injections (the PRIMARY
/// injection stays identity). Ports `_step_cache_key` (hand-built deterministic
/// string instead of a JSON dump). The scan filter is canonicalized via
/// `render_canonical`.
pub fn step_cache_key(step: &Step) -> String {
    match step {
        Step::SourceScan {
            datasource,
            scan,
            materialize,
            ..
        } => format!(
            "source_scan|ds={datasource}|mat={materialize}|{}",
            scan_spec_key(scan)
        ),
        Step::CollectDistinct {
            input, key, cap, ..
        } => format!("collect_distinct|in={input}|key={key}|cap={cap}"),
        Step::InjectedScan {
            datasource,
            scan,
            inject_column,
            keys_from,
            inject_column_ndv,
            ..
        } => format!(
            "injected_scan|ds={datasource}|col={inject_column}|keys={keys_from}|\
             ndv={inject_column_ndv:?}|{}",
            scan_spec_key(scan)
        ),
        Step::Ship {
            datasource,
            input,
            table,
        } => format!("ship|ds={datasource}|in={input}|table={table}"),
        Step::Merge {
            fragment, inputs, ..
        } => format!("merge|frag={fragment}|inputs={inputs:?}"),
        Step::Return { input } => format!("return|in={input}"),
    }
}

/// A scan spec's identity string EXCLUDING its mergeable `columns`, with the filter
/// canonicalized to stable SQL.
fn scan_spec_key(spec: &ScanSpec) -> String {
    let filter = match &spec.filter {
        Some(expr) => render_canonical(expr).unwrap_or_else(|_| format!("{expr:?}")),
        None => String::new(),
    };
    format!(
        "raw={:?}|schema={:?}|table={:?}|alias={:?}|filter={}|limit={:?}|\
         distinct={}|parallel={}|injected={:?}",
        spec.raw_sql,
        spec.schema,
        spec.table,
        spec.alias,
        filter,
        spec.limit,
        spec.distinct,
        spec.parallel,
        spec.injected_sql
    )
}

/// A source scan's alias-NEUTRAL sharing key. Two identical reads under different
/// aliases render different WHERE-qualifier SQL, so the key renders a CLONE under a
/// fixed `__cse__` alias; a non-injectable scan falls back to the literal step key.
/// Ports `_scan_share_key`.
pub fn scan_share_key(node: &PhysicalPlan, step: &Step) -> String {
    let PhysicalPlan::Scan(scan) = node else {
        return step_cache_key(step);
    };
    if !injectable_scan(node) {
        return step_cache_key(step);
    }
    let materialize = matches!(
        step,
        Step::SourceScan {
            materialize: true,
            ..
        }
    );
    let Ok(sql) = canonical_scan_sql(scan) else {
        return step_cache_key(step);
    };
    format!(
        "scan_share|op=source_scan|ds={}|schema={}|table={}|mat={materialize}|sql={sql}",
        scan.datasource, scan.schema_name, scan.table_name
    )
}

/// The scan rendered under a fixed `__cse__` alias, its filter references
/// requalified to match; used ONLY as a share-key string. Ports `_canonical_scan_sql`.
fn canonical_scan_sql(scan: &PhysicalScan) -> Result<String, super::error::StepError> {
    let mut clone = scan.clone();
    clone.alias = Some("__cse__".to_string());
    if let Some(filter) = &scan.filters {
        clone.filters = Some(requalify(filter.clone(), "__cse__"));
    }
    render_scan_sql(&clone)
}

/// Requalify every column reference of an expression to `table`. Mirrors the
/// Python `_replace_column_refs` remap, applied uniformly for the share key.
fn requalify(expr: Expr, table: &str) -> Expr {
    if let Expr::Column(col) = expr {
        return Expr::Column(ColumnRef {
            table: Some(table.to_string()),
            column: col.column,
            data_type: col.data_type,
        });
    }
    expr.map_children(&mut |child| requalify(child, table))
}

/// Widen an already-emitted structured scan's columns to cover a new consumer's
/// (consumers resolve by name, so extra columns are invisible). Ports
/// `_widen_cached_columns`. A raw (non-structured) spec is left untouched.
fn widen_cached_columns(cached: &mut Step, new_columns: &[String]) {
    let Some(scan) = scan_of_mut(cached) else {
        return;
    };
    if scan.table.is_none() {
        return;
    }
    for column in new_columns {
        if !scan.columns.contains(column) {
            scan.columns.push(column.clone());
        }
    }
}

/// Narrow an already-emitted injected scan's EXTRA injections to the intersection
/// with a new consumer's; the shared read must be a SUPERSET for every consumer, so
/// only common extras survive. Ports `_narrow_cached_extras`.
fn narrow_cached_extras(cached: &mut Step, new_extras: Option<&[super::types::ExtraInjection]>) {
    let Step::InjectedScan {
        extra_injections, ..
    } = cached
    else {
        return;
    };
    let new_extras = new_extras.unwrap_or(&[]);
    extra_injections.retain(|extra| new_extras.contains(extra));
}

/// A mutable view of a step's scan spec, when it has one.
fn scan_of_mut(step: &mut Step) -> Option<&mut ScanSpec> {
    match step {
        Step::SourceScan { scan, .. } | Step::InjectedScan { scan, .. } => Some(scan),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::{ExtraInjection, ScanSpec};
    use super::*;

    /// An injected scan on a probe base with a primary injection and given extras.
    fn injected(
        binding: &str,
        inject_column: &str,
        keys_from: &str,
        extras: Vec<ExtraInjection>,
    ) -> Step {
        let mut scan = ScanSpec::empty();
        scan.table = Some("store_sales".to_string());
        scan.columns = vec!["ss_sold_date_sk".to_string(), "ss_store_sk".to_string()];
        Step::InjectedScan {
            datasource: "duck".to_string(),
            scan,
            inject_column: inject_column.to_string(),
            keys_from: keys_from.to_string(),
            binding: binding.to_string(),
            inject_column_ndv: None,
            extra_injections: extras,
        }
    }

    fn extra(column: &str, keys_from: &str) -> ExtraInjection {
        ExtraInjection {
            column: column.to_string(),
            keys_from: keys_from.to_string(),
        }
    }

    #[test]
    fn cache_key_ignores_binding_and_extras_but_keeps_primary() {
        // Two injected scans identical but for binding and extras share a key.
        let a = injected("b1", "ss_sold_date_sk", "k1", vec![]);
        let b = injected(
            "b2",
            "ss_sold_date_sk",
            "k1",
            vec![extra("ss_store_sk", "k2")],
        );
        assert_eq!(step_cache_key(&a), step_cache_key(&b));
    }

    #[test]
    fn cache_key_differs_when_primary_keys_differ() {
        let a = injected("b1", "ss_sold_date_sk", "k1", vec![]);
        let b = injected("b2", "ss_sold_date_sk", "k2", vec![]);
        assert_ne!(step_cache_key(&a), step_cache_key(&b));
    }

    #[test]
    fn cache_key_differs_when_inject_column_differs() {
        let a = injected("b1", "ss_sold_date_sk", "k1", vec![]);
        let b = injected("b2", "ss_store_sk", "k1", vec![]);
        assert_ne!(step_cache_key(&a), step_cache_key(&b));
    }

    #[test]
    fn narrow_keeps_only_common_extras() {
        // Cached carries a store-key extra; the new consumer has none -> drop it.
        let mut cached = injected(
            "b1",
            "ss_sold_date_sk",
            "k1",
            vec![extra("ss_store_sk", "k2")],
        );
        let new_step = injected("b2", "ss_sold_date_sk", "k1", vec![]);
        let (_columns, extras) = super::merge_inputs(&new_step);
        narrow_cached_extras(&mut cached, extras.as_deref());
        match cached {
            Step::InjectedScan {
                extra_injections, ..
            } => assert!(extra_injections.is_empty(), "no common extras survive"),
            _ => unreachable!(),
        }
    }

    #[test]
    fn narrow_keeps_a_shared_extra() {
        let shared = extra("ss_store_sk", "k2");
        let mut cached = injected("b1", "ss_sold_date_sk", "k1", vec![shared.clone()]);
        let new_step = injected("b2", "ss_sold_date_sk", "k1", vec![shared.clone()]);
        let (_columns, extras) = super::merge_inputs(&new_step);
        narrow_cached_extras(&mut cached, extras.as_deref());
        match cached {
            Step::InjectedScan {
                extra_injections, ..
            } => assert_eq!(extra_injections, vec![shared]),
            _ => unreachable!(),
        }
    }

    #[test]
    fn widen_unions_new_columns_into_cached() {
        let mut cached = injected("b1", "ss_sold_date_sk", "k1", vec![]);
        // A new consumer that also reads ss_item_sk widens the shared read.
        widen_cached_columns(&mut cached, &["ss_item_sk".to_string()]);
        match cached {
            Step::InjectedScan { scan, .. } => {
                assert!(scan.columns.contains(&"ss_item_sk".to_string()));
                // Original columns are preserved.
                assert!(scan.columns.contains(&"ss_sold_date_sk".to_string()));
            }
            _ => unreachable!(),
        }
    }
}
