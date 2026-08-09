//! Stitch computation for the incremental paths index.
//!
//! A refresh changes one contiguous ENVELOPE of each touched actor's collapsed
//! stream: everything before the first delta influence and after the last is
//! byte-identical between the old stream and the merged stream. Because a path
//! window is at most `max_depth` events long, only windows overlapping that
//! envelope change. This module computes, per actor:
//!
//! - the merged collapsed stream and the old collapsed stream;
//! - the envelope, as the bounds left after stripping the longest common
//!   prefix and suffix (positions compare by `(code, ts)` - the two fields a
//!   sweep reads);
//! - the ADDITION ranges over the merged stream (windows the new segment
//!   counts) and the RETRACTION ranges over the old stream (windows older
//!   segments still count whose forms the envelope invalidated, cancelled by
//!   subtraction), each with the context the text needs around them.
//!
//! Ownership is per direction. Forward windows (unanchored, STARTING AT) are
//! keyed by their start position: a start within `max_depth - 1` BEFORE the
//! envelope reaches into it, so the forward range extends that far left.
//! Backward windows (ENDING AT) are keyed by their end position: an end within
//! `max_depth - 1` AFTER the envelope reaches back into it, so the backward
//! range extends that far right. On the old stream the envelope is empty for a
//! pure append, which leaves the backward retraction range empty - appending
//! never changes a window that ends at an old position.

/// One collapsed stream: adjacent duplicate codes merged, each kept position
/// carrying its first occurrence's timestamp.
pub(crate) struct CollapsedStream {
    pub(crate) codes: Vec<u32>,
    pub(crate) ts: Vec<i64>,
}

/// Collapse a `(code, ts)` sequence, keeping the first timestamp of each run
/// of equal adjacent codes.
pub(crate) fn collapse(events: &[(u32, i64)]) -> CollapsedStream {
    let mut codes: Vec<u32> = Vec::with_capacity(events.len());
    let mut ts: Vec<i64> = Vec::with_capacity(events.len());
    for (code, when) in events {
        if codes.last() != Some(code) {
            codes.push(*code);
            ts.push(*when);
        }
    }
    CollapsedStream { codes, ts }
}

/// A half-open position range `[lo, hi)` over one collapsed stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Span {
    pub(crate) lo: u32,
    pub(crate) hi: u32,
}

impl Span {
    /// Whether the range holds no positions.
    pub(crate) fn is_empty(self) -> bool {
        self.lo >= self.hi
    }
}

/// One actor's stitch: the text slice a segment carries and what it owns.
///
/// `text` bounds the slice of the stream the segment stores; `own_fwd` and
/// `own_bwd` are the owned position ranges inside it. `before` counts the
/// stream events preceding `text.lo`, so availability against the TRUE stream
/// bounds can be reconstructed (`after` is derivable from the stream length
/// and `text.hi`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct ActorStitch {
    pub(crate) text: Span,
    pub(crate) own_fwd: Span,
    pub(crate) own_bwd: Span,
}

/// The changed envelope of a touched actor: where the old and merged collapsed
/// streams stop agreeing, by `(code, ts)`.
///
/// `start` is the length of the longest common prefix; `end_old`/`end_merged`
/// are the envelope's exclusive ends on each stream (the common suffix begins
/// there). A pure append has `start == end_old`: the envelope is empty on the
/// old stream and covers exactly the appended positions on the merged one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Envelope {
    pub(crate) start: u32,
    pub(crate) end_old: u32,
    pub(crate) end_merged: u32,
}

impl Envelope {
    /// Whether the two streams agree everywhere: the envelope is empty on
    /// BOTH sides, so no window changed and neither stitch owns anything.
    /// The reach rules below anchor on the envelope boundary, so they must
    /// not run at all in this case.
    pub(crate) fn is_unchanged(&self) -> bool {
        self.start == self.end_old && self.start == self.end_merged
    }
}

/// Compute the envelope between the old and merged collapsed streams.
///
/// The common suffix is matched only past the common prefix, so the two never
/// overlap; ties (a position equal from both sides) resolve to the prefix.
pub(crate) fn envelope(old: &CollapsedStream, merged: &CollapsedStream) -> Envelope {
    let positions_agree =
        |a: usize, b: usize| old.codes[a] == merged.codes[b] && old.ts[a] == merged.ts[b];
    let shortest = old.codes.len().min(merged.codes.len());
    let mut prefix = 0usize;
    while prefix < shortest && positions_agree(prefix, prefix) {
        prefix += 1;
    }
    let mut suffix = 0usize;
    while suffix < shortest - prefix
        && positions_agree(
            old.codes.len() - 1 - suffix,
            merged.codes.len() - 1 - suffix,
        )
    {
        suffix += 1;
    }
    Envelope {
        start: u32::try_from(prefix).expect("stream fits u32"),
        end_old: u32::try_from(old.codes.len() - suffix).expect("stream fits u32"),
        end_merged: u32::try_from(merged.codes.len() - suffix).expect("stream fits u32"),
    }
}

/// The stitch over one stream given the envelope bounds on THAT stream.
///
/// `max_depth` is the dataset's window-length bound: forward ownership starts
/// `max_depth - 1` before the envelope (a window starting there reaches into
/// it), backward ownership ends `max_depth - 1` after it (a window ending
/// there reaches back into it), and the text extends `max_depth` past every
/// owned bound so any owned window of length at most `max_depth` stays inside
/// the slice.
fn stitch_over(stream_len: u32, envelope_lo: u32, envelope_hi: u32, max_depth: u32) -> ActorStitch {
    let reach = max_depth - 1;
    let own_fwd = Span {
        lo: envelope_lo.saturating_sub(reach),
        hi: envelope_hi,
    };
    let own_bwd = Span {
        lo: envelope_lo,
        hi: (envelope_hi + reach).min(stream_len),
    };
    let text = Span {
        lo: own_fwd.lo.saturating_sub(max_depth),
        hi: (own_bwd.hi + max_depth).min(stream_len),
    };
    ActorStitch {
        text,
        own_fwd,
        own_bwd,
    }
}

/// The addition stitch: what the new segment owns over the MERGED stream, or
/// `None` when the delta changed nothing about this actor's collapsed stream.
pub(crate) fn addition_stitch(
    merged_len: u32,
    envelope: Envelope,
    max_depth: u32,
) -> Option<ActorStitch> {
    if envelope.is_unchanged() {
        return None;
    }
    Some(stitch_over(
        merged_len,
        envelope.start,
        envelope.end_merged,
        max_depth,
    ))
}

/// The retraction stitch: what the retraction segment cancels over the OLD
/// stream, or `None` when nothing older segments count was invalidated (a new
/// actor, or an envelope whose forward reach is empty on an empty old side
/// with no owned positions at all).
pub(crate) fn retraction_stitch(
    old_len: u32,
    envelope: Envelope,
    max_depth: u32,
) -> Option<ActorStitch> {
    if envelope.is_unchanged() {
        return None;
    }
    let stitch = stitch_over(old_len, envelope.start, envelope.end_old, max_depth);
    if stitch.own_fwd.is_empty() && stitch.own_bwd.is_empty() {
        return None;
    }
    Some(stitch)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collapse merges adjacent runs and keeps first timestamps.
    #[test]
    fn collapse_keeps_first_of_each_run() {
        let collapsed = collapse(&[(3, 10), (3, 11), (5, 12), (3, 13), (3, 14)]);
        assert_eq!(collapsed.codes, vec![3, 5, 3]);
        assert_eq!(collapsed.ts, vec![10, 12, 13]);
    }

    /// A pure append leaves an empty envelope on the old stream and covers
    /// exactly the appended positions on the merged one.
    #[test]
    fn append_envelope_is_the_tail() {
        let old = collapse(&[(1, 10), (2, 20), (3, 30)]);
        let merged = collapse(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
        let env = envelope(&old, &merged);
        assert_eq!(
            env,
            Envelope {
                start: 3,
                end_old: 3,
                end_merged: 5
            }
        );
    }

    /// A boundary collapse (delta's first event merges into the old last) is
    /// part of the common prefix: the kept position has the old timestamp.
    #[test]
    fn boundary_collapse_stays_in_the_prefix() {
        let old = collapse(&[(1, 10), (2, 20)]);
        let merged = collapse(&[(1, 10), (2, 20), (2, 25), (3, 30)]);
        let env = envelope(&old, &merged);
        assert_eq!(
            env,
            Envelope {
                start: 2,
                end_old: 2,
                end_merged: 3
            }
        );
    }

    /// A late arrival in the middle bounds the envelope around the insertion,
    /// leaving the shared prefix and suffix outside it.
    #[test]
    fn late_arrival_bounds_the_envelope() {
        let old = collapse(&[(1, 10), (2, 20), (3, 30), (4, 40)]);
        let merged = collapse(&[(1, 10), (2, 20), (9, 25), (3, 30), (4, 40)]);
        let env = envelope(&old, &merged);
        assert_eq!(
            env,
            Envelope {
                start: 2,
                end_old: 2,
                end_merged: 3
            }
        );
    }

    /// A late arrival that BREAKS an old collapse re-expands the run: the old
    /// stream's single position becomes two in the merged one, and the
    /// envelope covers the re-expansion.
    #[test]
    fn broken_collapse_lands_in_the_envelope() {
        let old = collapse(&[(1, 10), (2, 20), (2, 25), (3, 30)]);
        assert_eq!(old.codes, vec![1, 2, 3]);
        let merged = collapse(&[(1, 10), (2, 20), (7, 22), (2, 25), (3, 30)]);
        assert_eq!(merged.codes, vec![1, 2, 7, 2, 3]);
        let env = envelope(&old, &merged);
        assert_eq!(
            env,
            Envelope {
                start: 2,
                end_old: 2,
                end_merged: 4
            }
        );
    }

    /// The append stitch: forward ownership reaches `max_depth - 1` into the
    /// old tail, backward ownership covers only the appended positions, and
    /// the retraction owns the tail forward with nothing backward.
    #[test]
    fn append_stitch_ownership() {
        let env = Envelope {
            start: 100,
            end_old: 100,
            end_merged: 120,
        };
        let addition = addition_stitch(120, env, 8).expect("append changes the stream");
        assert_eq!(addition.own_fwd, Span { lo: 93, hi: 120 });
        assert_eq!(addition.own_bwd, Span { lo: 100, hi: 120 });
        assert_eq!(addition.text, Span { lo: 85, hi: 120 });
        let retraction = retraction_stitch(100, env, 8).expect("tail retraction exists");
        assert_eq!(retraction.own_fwd, Span { lo: 93, hi: 100 });
        assert!(retraction.own_bwd.is_empty());
        assert_eq!(retraction.text, Span { lo: 85, hi: 100 });
    }

    /// A brand-new actor: no old stream, no retraction, the addition owns its
    /// whole stream in both directions.
    #[test]
    fn new_actor_has_no_retraction() {
        let old = collapse(&[]);
        let merged = collapse(&[(1, 10), (2, 20)]);
        let env = envelope(&old, &merged);
        assert_eq!(
            env,
            Envelope {
                start: 0,
                end_old: 0,
                end_merged: 2
            }
        );
        assert!(retraction_stitch(0, env, 8).is_none());
        let addition = addition_stitch(2, env, 8).expect("a new actor changes the stream");
        assert_eq!(addition.own_fwd, Span { lo: 0, hi: 2 });
        assert_eq!(addition.own_bwd, Span { lo: 0, hi: 2 });
        assert_eq!(addition.text, Span { lo: 0, hi: 2 });
    }

    /// An untouched stream (delta collapsed entirely into existing positions)
    /// yields an empty envelope and no retraction; the addition owns nothing.
    #[test]
    fn unchanged_stream_owns_nothing() {
        let old = collapse(&[(1, 10), (2, 20)]);
        let merged = collapse(&[(1, 10), (2, 20)]);
        let env = envelope(&old, &merged);
        assert_eq!(env.start, 2);
        assert_eq!(env.end_old, 2);
        assert_eq!(env.end_merged, 2);
        assert!(env.is_unchanged());
        assert!(addition_stitch(2, env, 8).is_none());
        assert!(retraction_stitch(2, env, 8).is_none());
    }
}
