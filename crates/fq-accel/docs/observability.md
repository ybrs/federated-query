[Accelerator documentation](../README.md)

# Observing reuse and estimated savings

Every SERVED substitution advances the view's counters, visible in
`SHOW MATERIALIZED VIEWS` (columns: `name, rows, bytes, created, refreshed,
use_count, cost_saved`). Starting from a fresh `region_rev`:

```text
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=0  cost_saved=0.0

-- two substituted runs of the exact aggregate query
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=2  cost_saved=18.0

-- an EXPLAIN plans but does not serve, so it records nothing
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=2  cost_saved=18.0
```

`cost_saved` accrues the gate's estimated saving per reuse: recompute rows minus
stored rows, here 12 - 3 = 9 per run, so 18 after two runs. The counters persist
in the registry, so a NEW runtime over the same store reads them back:

```text
-- fresh process, same store
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=2  cost_saved=18.0
```

A note on measured wins. `cost_saved` is an ESTIMATE in ROW units (input rows
elided minus stored rows read), not a measured time. This tutorial deliberately
quotes no latency numbers: per the repo's rule, engine perf claims come only
from `benchmarks/perf_compare`. For a like-for-like measured comparison of the
same store machinery (build once, serve warm, agree with a baseline signature),
follow a real benchmark harness that reports engine-versus-baseline ratios
rather than a counter.

[Accelerator documentation](../README.md)
