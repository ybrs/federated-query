"""Run every corpus query under every plan the optimizer can be made to choose.

`test_corpus` asserts one plan per query: whichever the optimizer picked. A plan
reachable only under different settings is therefore never executed, so a query
can be answered correctly by the plan that ran and wrongly by one that did not.
This runs those other plans and holds each to the same DuckDB reference.

One test per variant, each walking the whole corpus. That shape is deliberate:
changing a setting costs about what executing a query costs, so a variant applies
its settings once rather than once per query. A variant reports every query it
got wrong, not just the first, because knowing whether one plan is broken or
hundreds are is the difference between a defect and a design fault.
"""

import pytest

from tests.e2e_federated import harness
from tests.e2e_federated import plan_variants
from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import all_cases, case_table_specs
from tests.e2e_federated.compare import compare_tables
from tests.e2e_federated.oracle import build_oracle


@pytest.mark.parametrize(
    "variant_name,settings",
    plan_variants.VARIANTS,
    ids=[name for name, _settings in plan_variants.VARIANTS],
)
def test_every_plan_this_variant_reaches_agrees_with_the_oracle(
    variant_name, settings, engine, oracle_registry, default_plan_shapes
):
    """Walk the corpus under one variant, comparing each new plan to the oracle."""
    runtime = engine.runtime()
    variant = plan_variants.VariantRun(
        runtime, variant_name, settings, compare_tables
    )
    with variant as run:
        _walk_corpus(engine, run, oracle_registry, default_plan_shapes)
    print("\n" + run.summary())
    assert not run.failures, _failure_message(run)


def _walk_corpus(engine, run, oracle_registry, default_plan_shapes):
    """Check every case under every distribution against this variant."""
    for case in all_cases():
        if "expect_error" in case:
            continue
        oracle_table = _reference(oracle_registry, case)
        for variation in harness.variations(
            case_table_specs(case), case.get("min_sources", 1)
        ):
            _check_one(engine, run, case, variation, oracle_table,
                       default_plan_shapes)


def _check_one(engine, run, case, variation, oracle_table, default_plan_shapes):
    """Check one case under one distribution against this variant.

    The default plan's shape was taken once for the whole session, before any
    variant applied its settings, so deciding whether this variant changed the
    plan costs no settings change at all.
    """
    label = case["name"] + "[" + variation.name + "]"
    default_shape = default_plan_shapes.get(label)
    if default_shape is None:
        return
    environment = runtime_mod.open_variation(engine, variation)
    run.check(
        environment.render(case["query"]),
        default_shape,
        oracle_table,
        label,
        case.get("order_sensitive", False),
        case["name"],
        variation.name,
    )


def _reference(oracle_registry, case):
    """Return the oracle result for a case, computing it once per case."""
    key = case["name"]
    if key not in oracle_registry:
        oracle = build_oracle(case_table_specs(case))
        oracle_registry[key] = oracle.run(case["query"])
    return oracle_registry[key]


def _failure_message(run):
    """Build the message listing every query this variant answered differently."""
    lines = [
        "plan variant '" + run.name + "' disagrees with the oracle on "
        + str(len(run.failures)) + " of " + str(run.executed)
        + " queries whose plan it changed",
    ]
    for failure in run.failures[:40]:
        lines.append("  " + failure)
    if len(run.failures) > 40:
        lines.append("  ... " + str(len(run.failures) - 40) + " more")
    return "\n".join(lines)
