//! The paths kernel: the most common per-entity event sequences, computed in
//! one linear pass over the (entity, timestamp[, tiebreak])-sorted stream.
//!
//! Semantics (pinned):
//! - per entity, events are taken in time order; the order among DISTINCT
//!   events sharing one timestamp is the view's declared TIEBREAK column
//!   when there is one, and EVENT NAME ascending otherwise (event name also
//!   orders events sharing one tiebreak value). This is a DOCUMENTED
//!   DETERMINISTIC order, not a claim about true order: equal timestamps
//!   carry no order of their own, so the kernel pins one and identical data
//!   always yields identical paths.
//! - the path starts at the entity's first event named by STARTING AT (an
//!   entity with no such event contributes no path), or at the entity's
//!   first event when no anchor is declared;
//! - consecutive duplicate events collapse into one step (a reload storm is
//!   one step), which also makes equal-timestamp reorderings of the SAME
//!   event invisible;
//! - a path keeps at most MAX DEPTH collapsed steps; later events are
//!   dropped;
//! - identical paths count across entities (each entity contributes exactly
//!   one path) and the TOP k most common return, ordered by entities DESC
//!   then path ASC (the deterministic cut among equal counts).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef};
use fq_common::events::PathsSpec;

use crate::error::EventError;
use crate::sidecar::{worth_pruning, EntityBitmaps};
use crate::stream::{EventRow, EventStream, TiebreakRef};

/// The separator between a path's step names in the result's `path` column.
const STEP_SEPARATOR: &str = " -> ";

/// The paths result relation's fixed schema: one row per distinct path.
pub fn paths_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", ArrowType::Utf8, false),
        Field::new("entities", ArrowType::Int64, false),
        Field::new("depth", ArrowType::Int64, false),
    ]))
}

/// Run a path analysis over a sorted event stream: collect one collapsed,
/// depth-bounded path per entity, count identical paths, and return the TOP k
/// most common. A non-positive MAX DEPTH or TOP raises.
pub fn run_paths(
    stream: &EventStream,
    spec: &PathsSpec,
) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    require_positive(spec.max_depth, "MAX DEPTH")?;
    require_positive(spec.top, "TOP")?;
    let counts = count_paths(stream, spec)?;
    Ok((paths_schema(), vec![result_batch(counts, spec.top)?]))
}

/// Run an anchored path analysis using the anchor event's entity bitmap to
/// scan only the entities that have the anchor - the sole entities that can
/// contribute a path. The result is identical to `run_paths`.
///
/// Falls back to the plain scan when there is no anchor (every entity
/// contributes, so nothing prunes) or the stream is multi-batch (which a
/// whole-rewrite event view never produces).
pub fn run_paths_pruned(
    stream: &EventStream,
    spec: &PathsSpec,
    bitmaps: &EntityBitmaps,
) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    require_positive(spec.max_depth, "MAX DEPTH")?;
    require_positive(spec.top, "TOP")?;
    let Some(anchor) = spec.starting_at.as_deref() else {
        return run_paths(stream, spec);
    };
    let candidates = bitmaps.anchor_candidates(anchor);
    // Pruning only pays when the anchor is entity-selective and the stream is
    // one seekable batch; otherwise the plain scan is the reference answer.
    if !stream.is_single_batch() || !worth_pruning(candidates.len(), bitmaps.entity_count()) {
        return run_paths(stream, spec);
    }
    let counts = accumulate_paths(stream.candidate_rows(&candidates), spec)?;
    Ok((paths_schema(), vec![result_batch(counts, spec.top)?]))
}

/// Raise unless a clause's count is positive.
fn require_positive(count: i64, clause: &str) -> Result<(), EventError> {
    if count > 0 {
        return Ok(());
    }
    Err(EventError::Analysis(format!(
        "{clause} needs a positive count, got {count}"
    )))
}

/// Scan the stream once and count each entity's path: rows sharing an
/// (entity, timestamp) pair buffer into a tie group, the group flushes in
/// pinned order ((tiebreak, event name) - plain event name on a view with no
/// tiebreak), and the entity's collapsed steps tally at its boundary.
fn count_paths<'a>(
    stream: &'a EventStream,
    spec: &PathsSpec,
) -> Result<HashMap<Vec<&'a str>, i64>, EventError> {
    accumulate_paths(stream.rows(), spec)
}

/// Tally each entity's collapsed path from a row cursor - the shared body of
/// the full scan (`count_paths`) and the anchor-pruned scan
/// (`run_paths_pruned`); the two differ only in which rows the cursor yields.
fn accumulate_paths<'a>(
    rows: impl Iterator<Item = Result<EventRow<'a>, EventError>>,
    spec: &PathsSpec,
) -> Result<HashMap<Vec<&'a str>, i64>, EventError> {
    let mut counts: HashMap<Vec<&'a str>, i64> = HashMap::new();
    let mut collector = PathCollector::new(
        spec.starting_at.as_deref(),
        usize::try_from(spec.max_depth).expect("a positive MAX DEPTH fits usize"),
    );
    let mut group: Vec<(Option<TiebreakRef<'a>>, &'a str)> = Vec::new();
    let mut group_time = 0i64;
    for row in rows {
        let row = row?;
        if row.new_entity {
            flush_group(&mut group, &mut collector);
            collector.finish(&mut counts);
        } else if row.time != group_time {
            flush_group(&mut group, &mut collector);
        }
        group_time = row.time;
        group.push((row.tiebreak, row.event));
    }
    flush_group(&mut group, &mut collector);
    collector.finish(&mut counts);
    Ok(counts)
}

/// Drain one equal-(entity, timestamp) group into the collector in the pinned
/// tie order: the (tiebreak, event name) tuple order. On a view with no
/// tiebreak every tiebreak is None, so the order is event name alone.
fn flush_group<'a>(
    group: &mut Vec<(Option<TiebreakRef<'a>>, &'a str)>,
    collector: &mut PathCollector<'_, 'a>,
) {
    group.sort_unstable();
    for (_, event) in group.drain(..) {
        collector.push(event);
    }
}

/// One entity's path under construction: anchor gating, consecutive-duplicate
/// collapsing, and the depth bound.
struct PathCollector<'s, 'a> {
    /// The STARTING AT anchor event name; None anchors at the first event.
    start: Option<&'s str>,
    max_depth: usize,
    /// Whether the anchor was seen (immediately true with no anchor).
    anchored: bool,
    steps: Vec<&'a str>,
}

impl<'s, 'a> PathCollector<'s, 'a> {
    /// A collector for the first entity, ready to anchor.
    fn new(start: Option<&'s str>, max_depth: usize) -> Self {
        Self {
            start,
            max_depth,
            anchored: start.is_none(),
            steps: Vec::new(),
        }
    }

    /// Fold one event (already in pinned order) into the entity's path.
    fn push(&mut self, event: &'a str) {
        if !self.anchored {
            if self.start != Some(event) {
                return;
            }
            self.anchored = true;
        }
        // A consecutive duplicate collapses; a full path drops later events.
        if self.steps.last() == Some(&event) || self.steps.len() == self.max_depth {
            return;
        }
        self.steps.push(event);
    }

    /// Tally the finished entity's path (an entity that never anchored has no
    /// path and contributes nothing) and reset for the next entity.
    fn finish(&mut self, counts: &mut HashMap<Vec<&'a str>, i64>) {
        if !self.steps.is_empty() {
            *counts.entry(std::mem::take(&mut self.steps)).or_insert(0) += 1;
        }
        self.steps.clear();
        self.anchored = self.start.is_none();
    }
}

/// Assemble the result batch: paths ranked by entity count DESC then path
/// text ASC, cut to the TOP k, with each path's collapsed depth.
fn result_batch(counts: HashMap<Vec<&str>, i64>, top: i64) -> Result<RecordBatch, EventError> {
    let mut ranked = Vec::with_capacity(counts.len());
    for (steps, entities) in counts {
        let depth = i64::try_from(steps.len()).expect("a depth-bounded path length fits i64");
        ranked.push((steps.join(STEP_SEPARATOR), entities, depth));
    }
    ranked.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    ranked.truncate(usize::try_from(top).expect("a positive TOP fits usize"));
    let mut paths = Vec::with_capacity(ranked.len());
    let mut entity_counts = Vec::with_capacity(ranked.len());
    let mut depths = Vec::with_capacity(ranked.len());
    for (path, entities, depth) in ranked {
        paths.push(path);
        entity_counts.push(entities);
        depths.push(depth);
    }
    Ok(RecordBatch::try_new(
        paths_schema(),
        vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(entity_counts)),
            Arc::new(Int64Array::from(depths)),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::TimeUnit;
    use arrow::ipc::writer::FileWriter;
    use fq_common::events::EventRoleColumns;

    use crate::contract::build_event_view;

    /// The standard test roles over (user_id, ts, name), no tiebreak.
    fn roles() -> EventRoleColumns {
        EventRoleColumns {
            entity: "user_id".to_string(),
            timestamp: "ts".to_string(),
            event: "name".to_string(),
            tiebreak: None,
        }
    }

    /// The test roles with the Int32 `seq` column as the tiebreak.
    fn roles_with_tiebreak() -> EventRoleColumns {
        let mut roles = roles();
        roles.tiebreak = Some("seq".to_string());
        roles
    }

    /// A (user_id Int32, ts Timestamp(us), name Utf8, seq Int32) batch.
    fn batch(rows: &[(i32, i64, &str, i32)]) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", ArrowType::Int32, true),
            Field::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("name", ArrowType::Utf8, true),
            Field::new("seq", ArrowType::Int32, true),
        ]));
        let mut users = Vec::new();
        let mut times = Vec::new();
        let mut names = Vec::new();
        let mut seqs = Vec::new();
        for (user, time, name, seq) in rows {
            users.push(*user);
            times.push(*time);
            names.push(*name);
            seqs.push(*seq);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(users)),
                Arc::new(TimestampMicrosecondArray::from(times)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(seqs)),
            ],
        )
        .expect("batch");
        (schema, batch)
    }

    /// Build (sort) and open a stream over the rows under the given roles -
    /// the same establish-then-scan pair the runtime performs.
    fn stream(rows: &[(i32, i64, &str, i32)], roles: &EventRoleColumns) -> EventStream {
        let (schema, data) = batch(rows);
        let sorted = build_event_view("ev", &schema, &[data], roles).expect("build");
        EventStream::open("ev", &schema, &sorted, roles).expect("open")
    }

    /// A spec with no anchor and generous bounds.
    fn spec() -> PathsSpec {
        PathsSpec {
            view: "ev".to_string(),
            starting_at: None,
            max_depth: 10,
            top: 10,
        }
    }

    /// Flatten a paths result to (path, entities, depth) tuples.
    fn rows(result: &[RecordBatch]) -> Vec<(String, i64, i64)> {
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path Utf8");
        let entities = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("entities Int64");
        let depths = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("depth Int64");
        let mut flattened = Vec::new();
        for row in 0..batch.num_rows() {
            flattened.push((
                paths.value(row).to_string(),
                entities.value(row),
                depths.value(row),
            ));
        }
        flattened
    }

    #[test]
    fn consecutive_duplicates_collapse_into_one_step() {
        // u1's reload storm (view, view, view) is one step; the checkout
        // after it is the second.
        let stream = stream(
            &[
                (1, 10, "view", 0),
                (1, 20, "view", 0),
                (1, 30, "view", 0),
                (1, 40, "checkout", 0),
            ],
            &roles(),
        );
        let (_, result) = run_paths(&stream, &spec()).expect("paths");
        assert_eq!(rows(&result), vec![("view -> checkout".to_string(), 1, 2)]);
    }

    #[test]
    fn equal_timestamp_ties_order_by_event_name_without_a_tiebreak() {
        // u1's 'b' and 'a' share a timestamp: the pinned fallback order is
        // event name ascending, so the path is a -> b -> c.
        let stream = stream(
            &[(1, 10, "b", 0), (1, 10, "a", 0), (1, 20, "c", 0)],
            &roles(),
        );
        let (_, result) = run_paths(&stream, &spec()).expect("paths");
        assert_eq!(rows(&result), vec![("a -> b -> c".to_string(), 1, 3)]);
    }

    #[test]
    fn a_declared_tiebreak_orders_equal_timestamp_ties() {
        // The same rows under a tiebreak that says b happened first: the
        // declared order beats the event-name fallback.
        let stream = stream(
            &[(1, 10, "b", 1), (1, 10, "a", 2), (1, 20, "c", 3)],
            &roles_with_tiebreak(),
        );
        let (_, result) = run_paths(&stream, &spec()).expect("paths");
        assert_eq!(rows(&result), vec![("b -> a -> c".to_string(), 1, 3)]);
    }

    #[test]
    fn max_depth_truncates_and_bounds_the_depth_column() {
        let stream = stream(
            &[
                (1, 10, "a", 0),
                (1, 20, "b", 0),
                (1, 30, "c", 0),
                (1, 40, "d", 0),
            ],
            &roles(),
        );
        let mut shallow = spec();
        shallow.max_depth = 2;
        let (_, result) = run_paths(&stream, &shallow).expect("paths");
        assert_eq!(rows(&result), vec![("a -> b".to_string(), 1, 2)]);
    }

    #[test]
    fn starting_at_anchors_each_entity_and_drops_the_unanchored() {
        // u1 browses before signing up: the path starts at signup. u2 never
        // signs up: no path at all.
        let stream = stream(
            &[
                (1, 10, "browse", 0),
                (1, 20, "signup", 0),
                (1, 30, "purchase", 0),
                (2, 10, "browse", 0),
                (2, 20, "purchase", 0),
            ],
            &roles(),
        );
        let mut anchored = spec();
        anchored.starting_at = Some("signup".to_string());
        let (_, result) = run_paths(&stream, &anchored).expect("paths");
        assert_eq!(
            rows(&result),
            vec![("signup -> purchase".to_string(), 1, 2)]
        );
    }

    #[test]
    fn paths_rank_by_entities_desc_then_path_and_top_cuts() {
        // Two users share a -> b; one each walks b -> a and a -> c. Equal
        // counts order by path text; TOP 2 keeps the first two rows.
        let stream = stream(
            &[
                (1, 10, "a", 0),
                (1, 20, "b", 0),
                (2, 10, "a", 0),
                (2, 20, "b", 0),
                (3, 10, "b", 0),
                (3, 20, "a", 0),
                (4, 10, "a", 0),
                (4, 20, "c", 0),
            ],
            &roles(),
        );
        let (_, result) = run_paths(&stream, &spec()).expect("paths");
        assert_eq!(
            rows(&result),
            vec![
                ("a -> b".to_string(), 2, 2),
                ("a -> c".to_string(), 1, 2),
                ("b -> a".to_string(), 1, 2),
            ]
        );
        let mut two = spec();
        two.top = 2;
        let (_, cut) = run_paths(&stream, &two).expect("paths");
        assert_eq!(rows(&cut).len(), 2);
        assert_eq!(rows(&cut)[1].0, "a -> c");
    }

    #[test]
    fn an_empty_view_yields_an_empty_result() {
        let stream = stream(&[], &roles());
        let (schema, result) = run_paths(&stream, &spec()).expect("paths");
        assert_eq!(schema.field(0).name(), "path");
        assert_eq!(rows(&result).len(), 0);
    }

    #[test]
    fn non_positive_bounds_raise() {
        let stream = stream(&[(1, 10, "a", 0), (1, 20, "b", 0)], &roles());
        let mut zero_depth = spec();
        zero_depth.max_depth = 0;
        assert!(run_paths(&stream, &zero_depth).is_err());
        let mut zero_top = spec();
        zero_top.top = 0;
        assert!(run_paths(&stream, &zero_top).is_err());
    }

    /// The result batches serialized as Arrow IPC bytes, for byte-identity
    /// assertions.
    fn ipc_bytes(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = FileWriter::try_new(&mut bytes, schema).expect("ipc writer");
        for batch in batches {
            writer.write(batch).expect("write batch");
        }
        writer.finish().expect("finish ipc");
        drop(writer);
        bytes
    }

    #[test]
    fn permuted_equal_timestamp_input_orders_yield_byte_identical_paths() {
        // A fixture full of equal-timestamp ties: u1 and u2 each emit three
        // distinct events at one instant (plus a distinct-tiebreak pair),
        // u3 adds an equal-timestamp duplicate pair. Every input permutation
        // must produce byte-identical results - with a tiebreak (the declared
        // order) and without one (the event-name fallback).
        let fixture: Vec<(i32, i64, &str, i32)> = vec![
            (1, 10, "c", 1),
            (1, 10, "a", 2),
            (1, 10, "b", 3),
            (1, 20, "d", 4),
            (2, 10, "b", 1),
            (2, 10, "c", 2),
            (2, 10, "a", 3),
            (3, 10, "x", 1),
            (3, 10, "x", 2),
            (3, 20, "y", 3),
        ];
        for view_roles in [roles(), roles_with_tiebreak()] {
            let mut reference: Option<Vec<u8>> = None;
            for permutation in 0..fixture.len() {
                // Rotations plus a full reversal cover shuffled arrivals.
                let mut permuted = fixture.clone();
                permuted.rotate_left(permutation);
                if permutation % 2 == 1 {
                    permuted.reverse();
                }
                let stream = stream(&permuted, &view_roles);
                let (schema, result) = run_paths(&stream, &spec()).expect("paths");
                let bytes = ipc_bytes(&schema, &result);
                match &reference {
                    None => reference = Some(bytes),
                    Some(expected) => assert_eq!(
                        expected, &bytes,
                        "permutation {permutation} diverged (tiebreak: {:?})",
                        view_roles.tiebreak
                    ),
                }
            }
        }
    }

    #[test]
    fn the_declared_tiebreak_changes_the_tie_order_the_fallback_would_pick() {
        // One fixture, two modes: the tiebreak says c-b-a, the fallback says
        // a-b-c. Pinning both proves the tiebreak is actually consulted.
        let fixture: Vec<(i32, i64, &str, i32)> =
            vec![(1, 10, "a", 3), (1, 10, "b", 2), (1, 10, "c", 1)];
        let with_tiebreak = stream(&fixture, &roles_with_tiebreak());
        let (_, declared) = run_paths(&with_tiebreak, &spec()).expect("paths");
        assert_eq!(rows(&declared)[0].0, "c -> b -> a");
        let without = stream(&fixture, &roles());
        let (_, fallback) = run_paths(&without, &spec()).expect("paths");
        assert_eq!(rows(&fallback)[0].0, "a -> b -> c");
    }
}
