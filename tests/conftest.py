"""Optional test-only routing of query execution through the Rust engine.

Set ``FEDQRS=1`` to make every ``Executor.execute_to_table`` call try the Rust
engine (``fedqrs``) first, falling back to the DuckDB merge engine when the plan
is not yet supported or a source cannot be read natively. This lets the whole
existing suite run against the Rust engine to measure coverage:

  - a plan the Rust engine runs and gets right -> the test passes on Rust;
  - a plan it runs and gets wrong -> the test FAILS (a real Rust bug, surfaced);
  - a plan it cannot represent / read -> it falls back, test passes on DuckDB,
    and the reason is tallied.

A session-end summary prints how many executions went through Rust vs fell back,
grouped by reason. Without ``FEDQRS`` set this file does nothing.
"""

import collections
import os

_ROUTING = {
    "rust": 0,
    "unsupported": 0,
    "error": 0,
    "reasons": collections.Counter(),
}


def _reason(exc) -> str:
    """A short, groupable label for why a plan fell back."""
    text = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
    return text[:70]


def pytest_configure(config):
    """Monkeypatch the executor to route through Rust when FEDQRS is set."""
    if not os.environ.get("FEDQRS"):
        return

    from federated_query.executor.executor import Executor
    from federated_query.executor.rust_ir import execute_via_rust, UnsupportedIR

    original = Executor.execute_to_table

    def routed(self, plan, query_executor=None):
        catalog = getattr(query_executor, "catalog", None)
        datasources = getattr(catalog, "datasources", None)
        if datasources:
            outcome = _run_via_rust(plan, list(datasources.values()))
            if outcome is not None:
                return outcome
        return original(self, plan, query_executor=query_executor)

    Executor.execute_to_table = routed


def _run_via_rust(plan, datasources):
    """Try the Rust engine; return its table, or None to fall back."""
    from federated_query.executor.rust_ir import execute_via_rust, UnsupportedIR

    try:
        table = execute_via_rust(plan, datasources)
        _ROUTING["rust"] += 1
        return table
    except UnsupportedIR as exc:
        _ROUTING["unsupported"] += 1
        _ROUTING["reasons"][_reason(exc)] += 1
        return None
    except Exception as exc:  # noqa: BLE001 - measurement fallback, intentional
        _ROUTING["error"] += 1
        _ROUTING["reasons"]["err: " + _reason(exc)] += 1
        return None


def pytest_sessionfinish(session, exitstatus):
    """Print the Rust-routing coverage summary at the end of the run."""
    if not os.environ.get("FEDQRS"):
        return
    total = _ROUTING["rust"] + _ROUTING["unsupported"] + _ROUTING["error"]
    print("\n\n=== FEDQRS routing coverage ===")
    print(
        f"executions routed to Rust: {_ROUTING['rust']}  |  "
        f"fell back (unsupported): {_ROUTING['unsupported']}  |  "
        f"fell back (error): {_ROUTING['error']}  |  total: {total}"
    )
    print("top fallback reasons:")
    for reason, count in _ROUTING["reasons"].most_common(25):
        print(f"  {count:5}  {reason}")
    with open("/tmp/fedqrs_routing.txt", "w") as handle:
        handle.write(repr(dict(_ROUTING)))
