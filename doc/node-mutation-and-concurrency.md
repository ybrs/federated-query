# Node mutation and concurrency

The one settled way to transform plan / expression nodes, and why it is also the
right choice for a multi-threaded engine. Enforced by `tools/fq-lint`.

## The rule

To change a node, **mutate the fields that change and leave the rest alone.**
Never rebuild a node by re-listing every field.

Three permitted forms, cheapest first:

1. **In-place through `&mut`** - when you hold `&mut node`:
   ```rust
   sort.limit = new_limit;              // touches one field
   ```
2. **`update!` (own-and-mutate)** - when you own the node by value (the common
   case: rules take `plan: LogicalPlan`):
   ```rust
   LogicalPlan::Filter(update!(filter, { predicate = new_pred }))
   ```
   `update!` overwrites only the named fields and preserves the rest - the Rust
   analogue of pydantic `model_copy(update=...)`. See `fq_common::update`.
3. **Genuine construction** - a brand-new node (the parser building from SQL) or
   a changed enum variant (`Filter` -> `Scan`). A plain struct literal, allowed
   only with >= 2 comment lines justifying it (fq-lint FQ-CONSTRUCT).

Forbidden: `Filter { predicate: new, ..re-list-every-other-field }`. On any change
you retype every field, and one wrong value (a reset estimate, a dropped column
list) compiles clean and ships a wrong answer. This is the exact class of bug that
motivated the rule (see the Python `.create`/`model_copy` history and q18).

Also forbidden: `.clone()` of a subtree to transform it (fq-lint FQ-CLONE). A
`LogicalPlan::clone()` deep-copies every boxed descendant - the only genuinely
expensive operation here. Own the node or take `&mut` instead of cloning.

## Cost, precisely (nothing is free)

- **In-place `n.field = x`**: drops the old field, moves in the new. Touches one
  field. Cheapest.
- **`update!` / move-construct**: a move of the whole struct = a memcpy of its
  inline bytes (fields + child `Box` pointers). The compiler routinely elides the
  move to in-place, but Rust does not guarantee it. Cheap, not zero.
- **`.clone()`**: recursively heap-allocates every boxed descendant, then frees
  them. The one operation that touches the allocator. There are only a handful;
  keep it that way.

Most `LogicalPlan` variants are stored INLINE in the enum (only large ones like
`Scan` are boxed), so constructing a node value does not hit the allocator - it is
a stack copy. The allocator cost lives in `.clone()`, not construction.

## Why this is the right choice for a multi-threaded engine

Moving to Rust unlocks real threads (no GIL). The mutation idiom does not stand in
the way, because threading happens at boundaries OUTSIDE a single tree's
transformation:

1. **Concurrent queries** (the server win): each query is planned on its own
   thread, each **owning its own plan tree**. Our node structs are plain owned
   data (`String`/`Vec`/`Box`/enums, no `Rc`/`RefCell` in the tree), so a whole
   tree is `Send` - it moves to a worker thread for free. In-place mutation on an
   owned tree is exactly right here.
2. **Parallel execution of one query** (the data-plane win): scans/joins/aggregates
   across cores. Already handled by the DataFusion + tokio engine (fq-exec), which
   `Arc`s its OWN execution operators. Our `PhysicalPlan` is consumed to build that
   execution; it is not shared across execution threads itself.
3. **Parallel optimization of a single tree**: the only thing in-place gives up.
   Trees are tens of nodes, rules interact through a fixpoint, the payoff is
   microseconds - even DataFusion optimizes single-threaded. Not a real workload.
4. **Caching / sharing a finished plan**: `Arc` the FINISHED plan and execute it
   read-only. Orthogonal to how it was built: build in-place (owned), then `Arc`
   the result. You never mutate a shared plan.

So owned + `Send` (move the tree to a thread) beats shared + `Arc` (atomic
refcount traffic on every node) at the granularities that matter. `Arc` is used
only where there is GENUINE sharing: a CTE body referenced N times within one
plan, a cached finished plan, and execution (DataFusion's `Arc`, downstream).

## What we deliberately did NOT copy from DataFusion

DataFusion makes every `LogicalPlan` child an `Arc` (51 fields, 0 `Box`) and
rewrites purely functionally (`map_children` takes `self` by value, returns
`Transformed<T>` with a changed-flag; owned children come from `Arc::unwrap_or_clone`
- copy-on-write). That is correct for a LIBRARY whose plans cross threads and are
re-optimized while shared - it needs `Arc` anyway. As a single application that
owns one plan per query, we would pay atomic refcount traffic on every node for
sharing we do not use. So: owned structs + in-place mutation, `Arc` only where
sharing is real. (DataFusion's split is itself instructive: it `Arc`s plans but
`Box`es expressions - 50 `Box`, 1 `Arc` - because expressions are small and owned.)

## The one follow-up if we ever plan on worker threads

The optimizer currently holds `Rc<RefCell<CostModel>>` (not `Send`) - a
planning-time helper, not part of the tree. To move planning itself onto a worker
thread, change it to `Arc<...>`. Small, and only needed the day we do it.
