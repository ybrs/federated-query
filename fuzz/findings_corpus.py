"""Parking lot for triaged fuzzer findings, run as an e2e corpus module.

The fuzzer writes each raw finding to ``fuzz/artifacts/``. A finding that triage
confirms is a real engine bug is copied here as a normal corpus case so the e2e
suite guards it: a confirmed wrong-answer case lands in ``SUSPECTED_ENGINE_BUGS``
(kept out of ``CASES`` so the suite stays green) with its ``finding`` text and
tracker note, and is promoted into ``CASES`` once the bug is fixed. A confirmed
supported shape that merely lacked coverage lands directly in ``CASES``.

This module is registered in ``tests/e2e_federated/cases.py`` as the
``fuzz_findings`` corpus and starts empty, so ``FEDQ_E2E_CORPUS=fuzz_findings``
collects it cleanly until the first finding is parked.
"""

CASES = []

SUSPECTED_ENGINE_BUGS = {
    "fuzz_right_join_null_rejecting_where_extra_rows": {
        "name": "fuzz_right_join_null_rejecting_where_extra_rows",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT r0.val AS c0, r0.k AS c1 FROM {t_null_a} r0 "
            "RIGHT JOIN {t_null_b} r1 ON r0.k = r1.k "
            "WHERE r0.k IS NOT NULL AND r1.id > 3"
        ),
        "finding": (
            "Every cross-source placement returns 1 row where the oracle "
            "returns 0; oracle_single_duck agrees with the oracle, so the "
            "split-placement plan mishandles the null-rejecting WHERE on the "
            "null-extended side of the RIGHT join. Replay: python -m "
            "fuzz.run_fuzz --seed 3 --profile deep --replay 3096."
        ),
    },
    "fuzz_right_join_null_rejecting_where_extra_rows_v2": {
        "name": "fuzz_right_join_null_rejecting_where_extra_rows_v2",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT r0.val AS c0, r0.k AS c1 FROM {t_null_a} r0 "
            "RIGHT JOIN {t_null_b} r1 ON r0.k = r1.k "
            "WHERE r0.k IS NOT NULL AND r1.id IS NOT NULL"
        ),
        "finding": (
            "Same shape as fuzz_right_join_null_rejecting_where_extra_rows "
            "with an IS NOT NULL second conjunct: engine 3 rows vs oracle 2 "
            "on every cross-source placement. Replay index 3097."
        ),
    },
}
