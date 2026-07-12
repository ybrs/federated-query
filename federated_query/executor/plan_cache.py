"""Cross-query physical-plan cache, keyed by the exact (rewritten) SQL text.

The exploratory workload re-fires identical statements - notebook cell
re-runs, dashboard refreshes, an analyst iterating around a fixed query - and
each execution today re-pays the whole planning pipeline
(parse/bind/decorrelate/optimize/physical: 11-26ms of a 27-61ms small query).
A hit skips all of it and goes straight to execution.

Correctness posture: the cache stores PLANS, never data. A hit RE-EXECUTES
the plan against the sources, so results always reflect current data; the
only thing that can go stale is plan QUALITY (a plan built before newer
learned statistics), and the TTL bounds that lag. This matches the session
posture everywhere else: catalog metadata and source statistics are already
session-cached, so a cached plan sees exactly the world the uncached pipeline
would.

Deliberate exclusions:
- EXPLAIN plans never cache: EXPLAIN execution mutates scan nodes
  (dynamic_filter_values) and must present CURRENT planning decisions.
- Keys are the EXACT rewritten SQL, not a constant-neutral template:
  the optimizer MANUFACTURES and COPIES literals (transitive constants,
  CTE union-filter pushdown), so substituting new constants into a cached
  plan by position is not order-safe. The exact key needs no parse to hit.

Physical plans are immutable after construction (the documented invariant the
per-node schema cache already relies on), so re-executing a cached plan is
exactly re-executing the same query.
"""

import os
import time
from collections import OrderedDict
from typing import Optional

from ..plan.physical import PhysicalPlanNode

# Entries older than this many seconds fall out on read: a TTL bounds how
# long a plan may lag newer learned statistics (speed, never correctness).
DEFAULT_TTL_SECONDS = 300.0

# The cache holds at most this many plans; past it the least-recently-USED
# entry evicts. Plans are node trees of the queries' own size - hundreds of
# them are megabytes, not gigabytes.
DEFAULT_MAX_ENTRIES = 256


class PlanCache:
    """An LRU + TTL map from rewritten SQL text to its physical plan."""

    def __init__(self, max_entries=DEFAULT_MAX_ENTRIES,
                 ttl_seconds=DEFAULT_TTL_SECONDS, clock=time.monotonic):
        """Size- and age-bounded cache; `clock` is injectable so tests control
        expiry without sleeping."""
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self.clock = clock
        # sql -> (plan, stored_at); OrderedDict gives LRU order for free.
        self.entries: "OrderedDict[str, tuple]" = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, sql: str) -> Optional[PhysicalPlanNode]:
        """The cached plan for this SQL, or None (absent or older than the
        TTL - an expired entry is dropped, not served)."""
        entry = self.entries.get(sql)
        if entry is None:
            self.misses += 1
            return None
        plan, stored_at = entry
        if self.clock() - stored_at > self.ttl_seconds:
            del self.entries[sql]
            self.misses += 1
            return None
        self.entries.move_to_end(sql)
        self.hits += 1
        return plan

    def put(self, sql: str, plan: PhysicalPlanNode) -> None:
        """Store a freshly built plan, evicting the least-recently-used entry
        past the size bound."""
        self.entries[sql] = (plan, self.clock())
        self.entries.move_to_end(sql)
        if len(self.entries) > self.max_entries:
            self.entries.popitem(last=False)


def plan_cache_from_env() -> Optional[PlanCache]:
    """The runtime's plan cache per environment: FEDQ_PLAN_CACHE=0 disables
    (returns None), FEDQ_PLAN_CACHE_TTL / FEDQ_PLAN_CACHE_SIZE override the
    defaults. On by default - a plan cache changes only latency, never
    results."""
    if os.environ.get("FEDQ_PLAN_CACHE", "1") == "0":
        return None
    ttl = float(os.environ.get("FEDQ_PLAN_CACHE_TTL", DEFAULT_TTL_SECONDS))
    size = int(os.environ.get("FEDQ_PLAN_CACHE_SIZE", DEFAULT_MAX_ENTRIES))
    return PlanCache(max_entries=size, ttl_seconds=ttl)
