---
name: code-index
description: Search this codebase by CALL GRAPH instead of by text. Use when the question is "what calls X", "what does X call", "can X reach Y", "where is X defined", or "is anything reachable from here that does Z" - reachability, blast radius, dead code, tracing a defect to its callers. Grep answers where a string appears; this answers what can actually run. Reach for it before grepping across crates, and whenever a grep came back empty but the behaviour is real.
---

# Searching this codebase by call graph

`fq-index` is a rust-analyzer-backed index of the workspace and its dependencies.
It resolves method calls to the actual method and follows trait impls, so it
answers questions grep cannot.

## Where things are

    tools/fq-index/target/release/fq-index    the binary
    tools/fq-index/fq-index.sqlite            workspace crates only
    tools/fq-index/fq-index-deps.sqlite       workspace + dependency bodies
    tools/fq-index/fq-index.log               build log

Run it from `tools/fq-index` so the default `--db` resolves, or pass `--db` with
an absolute path from anywhere.

Use the workspace index for "what does OUR code do" - it is small and fast. Use
the deps index when the answer depends on what a dependency does with something
we hand it.

## The commands

    fq-index defs      <pattern>              where a function is defined
    fq-index callers   <pattern>              which call sites reach a function
    fq-index calls     <pattern>              which calls a function makes
    fq-index ancestors <pattern>              every function that can transitively reach it
    fq-index reach --from <pattern> --to <pattern>    is there a path, and what is it
    fq-index coverage                         what the index holds and what it could not see

Patterns match against the item path, so `stats_sqlite_path`,
`Runtime::execute` and `fq_runtime::` all work.

## Worked example

The question that this tool was built for: the engine's state SQLite was being
truncated during long runs, and grepping `remove_file`, `File::create` and
`truncate` across four crates found nothing.

    ./target/release/fq-index reach --from 'Runtime::execute' --to 'remove_file'
    ./target/release/fq-index reach --from 'Runtime::from_config' --to 'remove_dir_all'
    ./target/release/fq-index ancestors 'File::create'

That found five destructive sinks reachable from query execution, each confined
to a different directory, and none able to touch the file in question - which
eliminated the whole workspace as a suspect and pointed the investigation at FFI
instead. A negative answer from this tool is a result, where a negative from grep
is only an absence of evidence.

## READ THE BLIND SPOTS BEFORE TRUSTING A NEGATIVE

`fq-index coverage` prints what the index could not see. A "not reachable"
answer is only as good as that list, so read it and say what it was when you
report a conclusion.

The categories, and how much they actually matter here:

- **Unread crates.** Coverage prints how many crates were read versus merely
  named. With the workspace index, no dependency body was read at all - a call
  into `object_store` or `rusqlite` stops at the boundary. Use the deps index if
  that boundary is where your answer lives.
- **FFI.** The SQLite C library and the DuckDB C++ library are opaque, as is any
  SQL string handed to them. No Rust call graph can see inside them.
- **Function pointers and closure values.** Reported as counts. A closure's BODY
  is indexed under its enclosing function, so a closure passed straight to
  `thread::spawn` is followed; what is opaque is a closure stored in one place
  and invoked from another.
- **Trait dispatch.** A call through `dyn Trait` expands only to implementations
  in crates that were read. Against the workspace index a `dyn ObjectStore` call
  reaches the trait method but not the concrete implementation.
- **Anything outside the process.** Another program, an operator, a test harness
  writing to the same file is not in any static index.

## Building or refreshing it

    ./target/release/fq-index --db fq-index.sqlite build --root /workspace --workspace
    ./target/release/fq-index --db fq-index-deps.sqlite build --root /workspace --workspace --deps

The workspace pass takes about a minute. The deps pass takes hours, because it
type-checks DataFusion, arrow and tokio.

Both are INCREMENTAL and RESUMABLE: each crate is committed as it finishes, so a
run that is killed leaves a usable index, and re-running the same command skips
what is already there. `--rebuild` starts over. Progress goes to the log file,
never to the terminal - `--log <path>`, defaulting beside the database - so
stdout stays clean enough to pipe.

Peak memory is about 3 GB and cannot be lowered: rust-analyzer's query cache has
no working eviction in the pinned version.
