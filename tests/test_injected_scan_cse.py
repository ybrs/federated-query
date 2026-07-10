"""The injected-scan CSE identity: EXTRA injections never block sharing (they
narrow to the intersection on a hit), while the PRIMARY injection stays part
of the identity. Pins the q70 duplicate-fact-read fix."""

from federated_query.executor.rust_ir import (
    _narrow_cached_extras,
    _step_cache_key,
)


def _injected_step(binding, extras=None, keys_from="b_keys"):
    """A minimal structured injected-scan step for identity tests."""
    step = {
        "op": "injected_scan",
        "datasource": "duck",
        "scan": {"table": "store_sales", "columns": ["ss_item_sk"]},
        "inject_column": "ss_sold_date_sk",
        "keys_from": keys_from,
        "binding": binding,
    }
    if extras is not None:
        step["extra_injections"] = extras
    return step


def test_extras_do_not_block_sharing():
    """Two reads with the same PRIMARY injection share one step even when one
    carries an extra IN list the other lacks."""
    plain = _injected_step("in_0")
    with_extra = _injected_step(
        "in_1", extras=[{"column": "ss_store_sk", "keys_from": "b_store"}]
    )
    assert _step_cache_key(plain) == _step_cache_key(with_extra)


def test_primary_injection_stays_identity():
    """Different key SOURCES for the primary injection must never merge - the
    primary carries the reduction contract."""
    first = _injected_step("in_0", keys_from="b_keys")
    second = _injected_step("in_1", keys_from="b_other")
    assert _step_cache_key(first) != _step_cache_key(second)


def test_narrow_keeps_only_common_extras():
    """On a hit the shared step keeps the INTERSECTION of extras: an extra one
    consumer lacks could drop rows that consumer needs."""
    shared = {"column": "ss_store_sk", "keys_from": "b_store"}
    only_first = {"column": "ss_hdemo_sk", "keys_from": "b_hd"}
    cached = _injected_step("in_0", extras=[shared, only_first])
    incoming = _injected_step("in_1", extras=[shared])
    _narrow_cached_extras(cached, incoming)
    assert cached["extra_injections"] == [shared]


def test_narrow_drops_extras_field_when_none_shared():
    """A consumer with NO extras narrows the shared read to none at all (the
    field disappears rather than carrying an empty list)."""
    cached = _injected_step(
        "in_0", extras=[{"column": "ss_store_sk", "keys_from": "b_store"}]
    )
    incoming = _injected_step("in_1")
    _narrow_cached_extras(cached, incoming)
    assert "extra_injections" not in cached
