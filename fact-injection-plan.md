# Injection through composite probes - plan

Goal: stop transferring whole fact tables when the reducing keys exist. The
SF10 outlier cluster (q05 24x, q70 20x, q39/q58/q31/q06/q44 ~17x) is ONE
defect: semi-join reduction only accepts a PLAIN SCAN as the probe, but in
every cluster query the fact sits inside a composite subtree, so the
reduction either flips to the useless direction (collect fact keys, inject
into the dim: q05, q39, q06) or declines entirely (q58). Measured at SF10:
q05 ships 28.8M+14.4M+7.2M rows then filters to 14 days; q39 ships 133M
inventory rows then filters to 52 dates; q58 ships three full facts for a
one-week domain.

ASCII only. Status: IN PROGRESS.

## Diagnosis (profiles + join shapes, SF10)

- q39: joins are (inventory x item) x date_dim(est 363) x warehouse. At the
  date_dim join the probe side is a HashJoin subtree -> not injectable ->
  date_dim becomes probe. The date keys should land in the INVENTORY SCAN.
- q58: build = date_dim x __subq (decorrelated scalar week), probe =
  fact x item. Composite probe -> _can_reduce says no -> no reduction.
- q05: probe = UNION ALL of (sales scan, returns scan) per channel ->
  not injectable -> reversed. Keys must inject into EVERY branch scan.
- q06: composite probe (fact under decorrelated-scalar join) + a key
  collection forced on a fat join output (Phase-B anchor gap).

## Design: trace the probe key to its base scan(s)

Extend the emitter (rust_ir.py; planner untouched) the same way Phase B
re-anchored key COLLECTION: `_probe_key_bases(probe_child, probe_key_expr)`
returns the base scan(s) that ORIGINATE the probe key column, descending
only where filtering base rows by a key superset cannot change the join
result above:

- PhysicalProjection / AliasedRelation / Filter / SingleRowGuard: descend
  when the key column maps through (bijective rename ok, expression no).
- PhysicalHashJoin: descend into the side whose column_aliases carries the
  key qualifier, but ONLY when that side is not null-extended by the join:
  inner -> either side; left -> left side only; right -> right side only;
  full -> neither; semi/anti -> left side only. (Removing rows lacking key
  k from a non-extended side only removes rows the outer INNER reduction
  join would eliminate anyway.)
- Union / SetOperation (UNION ALL only): every branch must trace to a base
  scan; inject the SAME keys binding into all of them. INTERSECT/EXCEPT:
  no descent (branch semantics differ).
- PhysicalAggregate: keep the existing _reducible_probe_base rule (inject
  below the aggregate only in the shape it already validates).
- Anything else (window, limit, remote query already collapsed): stop; the
  base is not traceable and the join emits unreduced, as today.

All-or-nothing: if any branch of a union fails to trace, the whole probe is
untraceable.

## Phases

A. Orientation: teach _can_reduce/_probe_preference/_probe_base_resolvable
   that a composite subtree with traceable probe-key bases IS an injectable
   probe, so the big fact side stays the probe when the dim is filtered.
B. Emission: _emit_probe injects the keys binding into every traced base
   scan (ctx.injected keyed by id(base) already exists; extend to a list),
   then emits the probe subtree normally so the wrappers read reduced
   bases.
C. Cost gate: _reduction_filters prices the fetched fraction with the
   TRACED BASE's estimates (fact scan rows), not the composite's (None).
D. Collection anchor (q06's 2.2s collect): already Phase B; verify the
   i_category case traces, extend if the qualifier is lost in decorrelated
   wrappers.

## Verification gates

Fast loop per phase: pytest (1278), then staged SF10 engine+compare on the
cluster (5,6,31,39,44,58,70,78,79) - minutes with the cached references.
Full gates when the cluster settles: SF0.1 / SF1 / SF10 tallies 99|0|0 and
TPC-H fedpgduck SF1 (1.82x baseline; reduction-heavy, most sensitive to
orientation changes).

Expected: q05 3.7s -> well under 1s (51M rows -> ~1M), q39 7.2s -> ~1.5s,
q58/q31/q06 similar fraction, q78's 13s likely shrinks below the SMJ
threshold entirely (its inputs shrink 10x).
