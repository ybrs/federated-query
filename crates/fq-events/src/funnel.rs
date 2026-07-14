//! The funnel kernel: ordered step conversion within a time window, computed
//! in one linear pass over the (entity, timestamp)-sorted stream.
//!
//! Semantics (pinned):
//! - an attempt starts at EVERY step-1 event of an entity;
//! - within an attempt anchored at t0, step k matches the earliest event
//!   named steps[k] with timestamp STRICTLY greater than the step k-1 match
//!   and at most t0 + window (window inclusive, anchored at step 1);
//! - ordering is non-strict: events between matched steps are ignored;
//! - equal timestamps never advance a funnel (tie order in storage is
//!   unspecified, so tie-sensitive semantics would be nondeterministic);
//! - an entity's depth is the maximum over all its attempts, counted once.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef};
use fq_common::events::FunnelSpec;

use crate::error::EventError;
use crate::sidecar::{worth_pruning, EntityBitmaps};
use crate::stream::EventStream;

/// The most steps a funnel can declare; step matches per event are tracked
/// in a 16-bit mask.
pub const MAX_STEPS: usize = 16;

/// The funnel result relation's fixed schema: one row per step.
pub fn funnel_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("step", ArrowType::Int64, false),
        Field::new("event", ArrowType::Utf8, false),
        Field::new("entities", ArrowType::Int64, false),
        Field::new("conversion_from_start", ArrowType::Float64, true),
        Field::new("conversion_from_previous", ArrowType::Float64, true),
    ]))
}

/// Run a funnel over a sorted event stream: per-step distinct-entity counts
/// (cumulative: an entity reaching step k counts at every step <= k) plus the
/// conversion ratios. A step count outside 2..=16 or a non-positive window
/// raises.
pub fn run_funnel(
    stream: &EventStream,
    spec: &FunnelSpec,
) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    require_step_count(spec.steps.len())?;
    let window = stream.scale().window_in_native(spec.within)?;
    let reached = count_entities_per_depth(stream, &spec.steps, window)?;
    Ok((funnel_schema(), vec![result_batch(&spec.steps, &reached)?]))
}

/// Run a funnel using the per-event-name entity bitmaps to skip work: step 1's
/// count is the exact popcount of its bitmap, and only entities with both a
/// step-1 and a step-2 event (the sole entities that can reach depth >= 2) are
/// scanned. The result is identical to `run_funnel` over the same stream.
///
/// A multi-batch stream (which a whole-rewrite event view never produces)
/// cannot be seeked by binary search, so it keeps the plain full scan.
pub fn run_funnel_pruned(
    stream: &EventStream,
    spec: &FunnelSpec,
    bitmaps: &EntityBitmaps,
) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    require_step_count(spec.steps.len())?;
    let candidates = bitmaps.funnel_candidates(&spec.steps[0], &spec.steps[1]);
    // Pruning only pays when it skips enough entities and the stream is one
    // seekable batch; otherwise the plain linear scan is the reference answer.
    if !stream.is_single_batch() || !worth_pruning(candidates.len(), bitmaps.entity_count()) {
        return run_funnel(stream, spec);
    }
    let window = stream.scale().window_in_native(spec.within)?;
    let reached = count_entities_pruned(stream, &spec.steps, window, bitmaps, &candidates)?;
    Ok((funnel_schema(), vec![result_batch(&spec.steps, &reached)?]))
}

/// The per-step counts computed from the bitmaps plus a candidate-only scan:
/// step 1 is the bitmap popcount; steps 2.. come from scanning only the
/// entities in `step1 & step2` and folding their depth into steps 2..=depth
/// (step 1 is already counted, so a candidate never re-increments it).
fn count_entities_pruned(
    stream: &EventStream,
    steps: &[String],
    window: i64,
    bitmaps: &EntityBitmaps,
    candidates: &crate::stream::EntitySet,
) -> Result<Vec<i64>, EventError> {
    let mut reached = vec![0i64; steps.len()];
    reached[0] = i64::try_from(bitmaps.event_entities(&steps[0])).expect("step-1 count fits i64");
    if candidates.is_empty() {
        return Ok(reached);
    }
    let mut entity_events: Vec<(i64, u16)> = Vec::new();
    for row in stream.candidate_rows(candidates) {
        let row = row?;
        if row.new_entity {
            tally_deep_steps(&mut reached, &entity_events, steps.len(), window);
            entity_events.clear();
        }
        let mask = step_mask(steps, row.event);
        if mask != 0 {
            entity_events.push((row.time, mask));
        }
    }
    tally_deep_steps(&mut reached, &entity_events, steps.len(), window);
    Ok(reached)
}

/// Fold one candidate entity's depth into steps 2..=depth. Step 1 is counted by
/// the bitmap popcount, so the range starts at index 1 (a depth-1 candidate
/// adds nothing here).
fn tally_deep_steps(reached: &mut [i64], events: &[(i64, u16)], step_count: usize, window: i64) {
    let depth = entity_depth(events, step_count, window);
    for step in reached.iter_mut().take(depth).skip(1) {
        *step += 1;
    }
}

/// Raise unless the declared step count is within 2..=16.
fn require_step_count(count: usize) -> Result<(), EventError> {
    if (2..=MAX_STEPS).contains(&count) {
        return Ok(());
    }
    Err(EventError::Analysis(format!(
        "a funnel takes 2 to {MAX_STEPS} steps, got {count}"
    )))
}

/// Scan the stream once and return, per step index, how many distinct
/// entities reached AT LEAST that step. Only step-relevant events are
/// buffered per entity; the buffer flushes at every entity boundary.
fn count_entities_per_depth(
    stream: &EventStream,
    steps: &[String],
    window: i64,
) -> Result<Vec<i64>, EventError> {
    let mut reached = vec![0i64; steps.len()];
    let mut entity_events: Vec<(i64, u16)> = Vec::new();
    for row in stream.rows() {
        let row = row?;
        if row.new_entity {
            tally_entity(&mut reached, &entity_events, steps.len(), window);
            entity_events.clear();
        }
        let mask = step_mask(steps, row.event);
        if mask != 0 {
            entity_events.push((row.time, mask));
        }
    }
    tally_entity(&mut reached, &entity_events, steps.len(), window);
    Ok(reached)
}

/// The bitmask of step indices whose declared event name equals `event`.
fn step_mask(steps: &[String], event: &str) -> u16 {
    let mut mask = 0u16;
    for (index, step) in steps.iter().enumerate() {
        if step == event {
            mask |= 1 << index;
        }
    }
    mask
}

/// Fold one entity's step-relevant events into the cumulative per-step
/// counts: its max depth d increments every step index below d.
fn tally_entity(reached: &mut [i64], events: &[(i64, u16)], step_count: usize, window: i64) {
    let depth = entity_depth(events, step_count, window);
    for step_reached in reached.iter_mut().take(depth) {
        *step_reached += 1;
    }
}

/// The deepest funnel step one entity reaches, over all its attempts. Each
/// step-1 event anchors an attempt; the attempt greedily matches each next
/// step at the earliest strictly-later event within the window. The cost is
/// O(step-1 occurrences x events inside the window) for this entity.
fn entity_depth(events: &[(i64, u16)], step_count: usize, window: i64) -> usize {
    let mut best = 0;
    for (start, &(anchor, mask)) in events.iter().enumerate() {
        if mask & 1 == 0 {
            continue;
        }
        // Saturation is exact here: a deadline past i64::MAX admits every
        // representable timestamp, which is what the true deadline admits.
        let deadline = anchor.saturating_add(window);
        best = best.max(attempt_depth(
            &events[start + 1..],
            anchor,
            deadline,
            step_count,
        ));
        if best == step_count {
            break;
        }
    }
    best
}

/// The depth one attempt reaches: starting after its anchor event, greedily
/// take the earliest event matching the next step strictly after the previous
/// match and no later than the deadline.
fn attempt_depth(rest: &[(i64, u16)], anchor: i64, deadline: i64, step_count: usize) -> usize {
    let mut depth = 1;
    let mut last_matched = anchor;
    for &(time, mask) in rest {
        if time > deadline || depth == step_count {
            break;
        }
        if (mask >> depth) & 1 == 1 && time > last_matched {
            depth += 1;
            last_matched = time;
        }
    }
    depth
}

/// Assemble the one result batch: step ordinals, step names, cumulative
/// entity counts, and the two conversion ratio columns (NULL on a zero
/// denominator - never a fabricated ratio; step 1 has no previous step).
fn result_batch(steps: &[String], reached: &[i64]) -> Result<RecordBatch, EventError> {
    let mut ordinals = Vec::with_capacity(steps.len());
    let mut names = Vec::with_capacity(steps.len());
    let mut from_start = Vec::with_capacity(steps.len());
    let mut from_previous = Vec::with_capacity(steps.len());
    for (index, name) in steps.iter().enumerate() {
        ordinals.push(i64::try_from(index).expect("step index fits i64") + 1);
        names.push(name.clone());
        from_start.push(ratio(reached[index], reached[0]));
        from_previous.push(if index == 0 {
            None
        } else {
            ratio(reached[index], reached[index - 1])
        });
    }
    Ok(RecordBatch::try_new(
        funnel_schema(),
        vec![
            Arc::new(Int64Array::from(ordinals)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(reached.to_vec())),
            Arc::new(Float64Array::from(from_start)),
            Arc::new(Float64Array::from(from_previous)),
        ],
    )?)
}

/// A conversion ratio, or None when the denominator is zero.
fn ratio(numerator: i64, denominator: i64) -> Option<f64> {
    if denominator == 0 {
        return None;
    }
    Some(numerator as f64 / denominator as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;
    use fq_common::events::{EventRoleColumns, EventWindow, WindowUnit};

    /// The standard test roles over (user_id, ts, name), no tiebreak.
    fn roles() -> EventRoleColumns {
        EventRoleColumns {
            entity: "user_id".to_string(),
            timestamp: "ts".to_string(),
            event: "name".to_string(),
            tiebreak: None,
        }
    }

    /// A sorted stream over (user_id Int32, ts Timestamp(us), name Utf8) rows.
    fn stream(rows: &[(i32, i64, &str)]) -> EventStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", ArrowType::Int32, true),
            Field::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("name", ArrowType::Utf8, true),
        ]));
        let mut users = Vec::new();
        let mut times = Vec::new();
        let mut names = Vec::new();
        for (user, time, name) in rows {
            users.push(*user);
            times.push(*time);
            names.push(*name);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(users)),
                Arc::new(TimestampMicrosecondArray::from(times)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("batch");
        EventStream::open("ev", &schema, &[batch], &roles()).expect("open")
    }

    /// One microsecond-scale hour, for readable fixture timestamps.
    const HOUR: i64 = 3600 * 1_000_000;

    /// A three-step signup -> activate -> purchase spec with a 2 HOURS window.
    fn spec() -> FunnelSpec {
        FunnelSpec {
            view: "ev".to_string(),
            steps: vec![
                "signup".to_string(),
                "activate".to_string(),
                "purchase".to_string(),
            ],
            within: EventWindow {
                count: 2,
                unit: WindowUnit::Hours,
            },
        }
    }

    /// The (entities, conversion_from_start, conversion_from_previous)
    /// columns of a funnel result, flattened per step.
    fn counts(result: &[RecordBatch]) -> Vec<(i64, Option<f64>, Option<f64>)> {
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        let entities = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("entities Int64");
        let start = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("from_start Float64");
        let previous = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("from_previous Float64");
        let mut rows = Vec::new();
        for row in 0..batch.num_rows() {
            let start_value = start.is_valid(row).then(|| start.value(row));
            let previous_value = previous.is_valid(row).then(|| previous.value(row));
            rows.push((entities.value(row), start_value, previous_value));
        }
        rows
    }

    #[test]
    fn hand_computed_six_user_funnel() {
        // Six users, hand-verifiable against a 2-hour window:
        // u1 completes within the window            -> depth 3
        // u2 activates 3h after signup (too late)   -> depth 1
        // u3 purchases BEFORE activating: the early purchase cannot match
        //    step 3 (not strictly after activate)   -> depth 2
        // u4 never signs up                         -> depth 0
        // u5 fails from its first signup but re-enters at 2.5h and completes
        //    from there                             -> depth 3 (re-entry)
        // u6 activates at the SAME instant as signup: a tie never advances
        //                                           -> depth 1
        let stream = stream(&[
            (1, 0, "signup"),
            (1, HOUR / 2, "activate"),
            (1, HOUR, "purchase"),
            (2, 0, "signup"),
            (2, 3 * HOUR, "activate"),
            (3, 0, "signup"),
            (3, HOUR / 3, "purchase"),
            (3, HOUR / 2, "activate"),
            (4, 0, "activate"),
            (4, HOUR / 2, "purchase"),
            (5, 0, "signup"),
            (5, 5 * HOUR / 2, "signup"),
            (5, 3 * HOUR, "activate"),
            (5, 7 * HOUR / 2, "purchase"),
            (6, 0, "signup"),
            (6, 0, "activate"),
        ]);
        let (_, result) = run_funnel(&stream, &spec()).expect("funnel");
        // Step 1: u1,u2,u3,u5,u6 = 5; step 2: u1,u3,u5 = 3; step 3: u1,u5 = 2.
        assert_eq!(
            counts(&result),
            vec![
                (5, Some(1.0), None),
                (3, Some(0.6), Some(0.6)),
                (2, Some(0.4), Some(2.0 / 3.0)),
            ]
        );
    }

    #[test]
    fn the_window_boundary_is_inclusive() {
        // u1 activates at EXACTLY signup + 2h: converts. u2 activates one
        // microsecond later: does not.
        let stream = stream(&[
            (1, 0, "signup"),
            (1, 2 * HOUR, "activate"),
            (2, 0, "signup"),
            (2, 2 * HOUR + 1, "activate"),
        ]);
        let (_, result) = run_funnel(&stream, &spec()).expect("funnel");
        assert_eq!(counts(&result)[1].0, 1);
    }

    #[test]
    fn intervening_events_do_not_break_the_funnel() {
        let stream = stream(&[
            (1, 0, "signup"),
            (1, 10, "browse"),
            (1, 20, "activate"),
            (1, 30, "browse"),
            (1, 40, "purchase"),
        ]);
        let (_, result) = run_funnel(&stream, &spec()).expect("funnel");
        assert_eq!(counts(&result)[2].0, 1);
    }

    #[test]
    fn an_empty_view_yields_zero_counts_and_null_conversions() {
        let stream = stream(&[]);
        let (_, result) = run_funnel(&stream, &spec()).expect("funnel");
        assert_eq!(
            counts(&result),
            vec![(0, None, None), (0, None, None), (0, None, None)]
        );
    }

    #[test]
    fn duplicate_step_names_need_two_occurrences() {
        // Steps ('view', 'view'): one 'view' event reaches depth 1 only; a
        // second, strictly later 'view' completes the pair.
        let two_views = FunnelSpec {
            view: "ev".to_string(),
            steps: vec!["view".to_string(), "view".to_string()],
            within: EventWindow {
                count: 2,
                unit: WindowUnit::Hours,
            },
        };
        let stream = stream(&[(1, 0, "view"), (2, 0, "view"), (2, 10, "view")]);
        let (_, result) = run_funnel(&stream, &two_views).expect("funnel");
        assert_eq!(counts(&result)[0].0, 2);
        assert_eq!(counts(&result)[1].0, 1);
    }

    #[test]
    fn step_count_and_window_bounds_raise() {
        let stream = stream(&[(1, 0, "signup")]);
        let mut one_step = spec();
        one_step.steps.truncate(1);
        assert!(run_funnel(&stream, &one_step).is_err());
        let mut too_many = spec();
        too_many.steps = vec!["s".to_string(); MAX_STEPS + 1];
        assert!(run_funnel(&stream, &too_many).is_err());
        let mut zero_window = spec();
        zero_window.within.count = 0;
        assert!(run_funnel(&stream, &zero_window).is_err());
    }

    #[test]
    fn date_timestamps_take_whole_day_windows() {
        // A DATE-typed timestamp column: windows count days exactly.
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", ArrowType::Int32, true),
            Field::new("ts", ArrowType::Date32, true),
            Field::new("name", ArrowType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(Date32Array::from(vec![100, 107, 200, 208])),
                Arc::new(StringArray::from(vec![
                    "signup", "activate", "signup", "activate",
                ])),
            ],
        )
        .expect("batch");
        let stream = EventStream::open("ev", &schema, &[batch], &roles()).expect("open");
        let mut days = spec();
        days.within = EventWindow {
            count: 7,
            unit: WindowUnit::Days,
        };
        // u1 activates on day 107 (inclusive boundary); u2 on day 208 (late).
        let (_, result) = run_funnel(&stream, &days).expect("funnel");
        assert_eq!(counts(&result)[1].0, 1);
        // A sub-day window unit over a DATE column raises.
        let mut hours = spec();
        hours.within = EventWindow {
            count: 48,
            unit: WindowUnit::Hours,
        };
        assert!(run_funnel(&stream, &hours).is_err());
    }
}
