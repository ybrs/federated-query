[Accelerator documentation](../README.md)

# Current limitations

- EXACT-MATCH ONLY. No predicate subsumption, no column-subset matching, no
  ordinal/alias canonicalization beyond what the optimizer already folds. A view
  serves only queries whose optimized subtree is structurally identical to its
  definition ([matching boundaries](matching-limits.md)). A different constant, column set, or filter, or a
  user table alias, all decline.
- PER-SESSION KILL SWITCH. `accelerator.enable_substitution` is a session
  setting, not a per-view or per-query control. You cannot disable substitution
  for one view while keeping it for another; it is all-or-nothing per session.
- ESTIMATED cost_saved. The counter is in ROW units from the cost model's
  estimate, not measured milliseconds. Do not read it as a timing.
- NO EVICTION. The store grows as you create and refresh views; there is no size
  budget and no automatic eviction yet. `DROP MATERIALIZED VIEW` is the only way
  to reclaim a view's chunks.
- ORDER-FIXING SHAPES ARE NEVER SUBSTITUTED. A subtree that fixes output order
  or row selection anywhere within it - a Sort, a LIMIT, or a scan with a folded
  ORDER BY / LIMIT, at the root OR nested under a Projection/Limit - always
  declines ([ordering and row selection](matching-limits.md#ordering-and-row-selection)). This is a correctness guard, not a missing optimization:
  a chunk read is an unordered SET, so serving such a shape could drop its order.
  You therefore cannot accelerate an ORDER BY / LIMIT view directly; put the
  ORDER BY in the OUTER query, above the view, so the plan re-sorts the
  substituted scan.

[Accelerator documentation](../README.md)
