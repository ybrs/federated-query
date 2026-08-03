//! A Filter above a projected RIGHT JOIN must reject null-extended rows.
//!
//! This mirrors, in plain DataFusion, exactly how the engine composes a
//! coordinator RIGHT JOIN: join two registered inputs on `k`, project the
//! join output with the left `k` under the bare name `k` (the right one
//! renamed away), register the projection as a view, filter the view on
//! `k IS NOT NULL`, and project the filtered view once more. A RIGHT JOIN
//! null-extends the left side of every unmatched right row, and the filter
//! names the LEFT `k`, so every null-extended row must be rejected: the
//! correct answer is the two matched rows and nothing else.
//!
//! Under DataFusion 54's stock physical optimizer this composition returns a
//! null-extended row: the physical FilterPushdown rule relocates the
//! above-join predicate below the join through the join-key equivalence,
//! which is invalid for an outer join's null-supplying side. The engine's
//! coordinator contexts therefore exclude those rules
//! (`coordinator_context` in src/engine.rs), and this test pins the exact
//! composition against the exact context construction the engine uses.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::JoinType;
use datafusion::datasource::view::ViewTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use fq_exec::engine::coordinator_physical_rules;

/// A context carrying the engine's own coordinator rule set, so this test runs
/// against the exact exclusion `coordinator_context` applies rather than a copy
/// of it that could drift.
fn context_without_physical_filter_pushdown() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rules(coordinator_physical_rules())
        .build();
    SessionContext::new_with_state(state)
}

/// t_null_a(k, val): keys 10 and 20 match, one NULL key, one unmatched 30.
fn left_table() -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("val", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(30)])),
            Arc::new(StringArray::from(vec![
                Some("a-ten"),
                Some("a-twenty"),
                Some("a-null"),
                Some("a-thirty"),
            ])),
        ],
    )
    .expect("left batch");
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("left table"))
}

/// t_null_b(id, k): ids 1 and 3 match; id 2 has a NULL key and id 4 an
/// unmatched key, so both null-extend the left side under a RIGHT JOIN.
fn right_table() -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(20), Some(40)])),
        ],
    )
    .expect("right batch");
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("right table"))
}

#[tokio::test]
async fn filter_over_projected_right_join_rejects_null_extended_rows() {
    let ctx = context_without_physical_filter_pushdown();
    ctx.register_table("in_left", left_table())
        .expect("register left");
    ctx.register_table("in_right", right_table())
        .expect("register right");

    let left = ctx.table("in_left").await.expect("left frame");
    let right = ctx.table("in_right").await.expect("right frame");
    let joined = left
        .join(right, JoinType::Right, &["k"], &["k"], None)
        .expect("join");
    let projected = joined
        .select(vec![
            col("in_left.k").alias("k"),
            col("in_left.val").alias("val"),
            col("in_right.k").alias("right_k"),
        ])
        .expect("project");

    let view_ctx = context_without_physical_filter_pushdown();
    view_ctx
        .register_table(
            "in_0",
            Arc::new(ViewTable::new(projected.into_unoptimized_plan(), None)),
        )
        .expect("register view");
    let filtered = view_ctx
        .table("in_0")
        .await
        .expect("view frame")
        .filter(col("in_0.k").is_not_null())
        .expect("filter");

    // The final projection layer, stacked exactly as the engine stacks it: a
    // THIRD context whose `in_0` is a view over the filtered plan, projected
    // to the query's output names, and only then collected - so DataFusion's
    // optimizer sees the whole nested-view plan at once.
    let project_ctx = context_without_physical_filter_pushdown();
    project_ctx
        .register_table(
            "in_0",
            Arc::new(ViewTable::new(filtered.into_unoptimized_plan(), None)),
        )
        .expect("register filtered view");
    let projected_out = project_ctx
        .table("in_0")
        .await
        .expect("filtered frame")
        .select(vec![col("in_0.val").alias("c0"), col("in_0.k").alias("c1")])
        .expect("output projection");

    let batches = projected_out.collect().await.expect("collect");
    let mut kept: Vec<Option<i64>> = Vec::new();
    for batch in &batches {
        let ks = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c1 column");
        for i in 0..ks.len() {
            kept.push(if ks.is_null(i) {
                None
            } else {
                Some(ks.value(i))
            });
        }
    }
    kept.sort();
    assert_eq!(
        kept,
        vec![Some(10), Some(20)],
        "null-extended rows must not pass `k IS NOT NULL`; got {kept:?}"
    );
}
