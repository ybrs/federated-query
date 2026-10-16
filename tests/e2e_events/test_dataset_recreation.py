"""Keep recreated event datasets independent of deleted datasets' warm caches."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

TESTS_DIR = Path(__file__).resolve().parents[1]
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

import fedq  # noqa: E402

PATHS = "PATHS ON recreated ENDING AT 'app_remove' DEPTH 8 TOP 20"
EXPECTED_OLD = [
    {"path_id": 1, "seq": 1, "event": "tutorial_begin", "occurrences": 2, "share": 1.0},
    {"path_id": 1, "seq": 2, "event": "app_remove", "occurrences": 2, "share": 1.0},
]
EXPECTED_NEW = [
    {
        "path_id": 1,
        "seq": 1,
        "event": "notification_receive",
        "occurrences": 1,
        "share": 0.5,
    },
    {"path_id": 1, "seq": 2, "event": "app_remove", "occurrences": 1, "share": 0.5},
    {"path_id": 2, "seq": 1, "event": "tutorial_begin", "occurrences": 1, "share": 0.5},
    {"path_id": 2, "seq": 2, "event": "app_remove", "occurrences": 1, "share": 0.5},
]
CHILD_QUERY = """
import json
import sys
sys.path.insert(0, sys.argv[2])
import fedq
import pyarrow as pa
runtime = fedq.Runtime(sys.argv[1])
for suffix in ("", " USING SCAN"):
    print(json.dumps(pa.table(runtime.execute(sys.argv[3] + suffix)).to_pylist()))
"""


def execute(runtime, sql):
    """Read the full SQL result through the public native-extension surface."""
    return pa.table(runtime.execute(sql)).to_pylist()


def create_dataset(runtime, changed):
    """Change one actor's events while preserving counts and dictionary order."""
    event = "notification_receive" if changed else "tutorial_begin"
    execute(
        runtime,
        (
            "CREATE EVENT DATASET recreated AS ("
            "SELECT actor, ts, event FROM (VALUES "
            "(0, 1, 'tutorial_begin'), (0, 2, 'app_remove'), "
            "(0, 3, 'notification_receive'), "
            "(1, 1, '%s'), (1, 2, 'app_remove')) AS v(actor, ts, event)"
            ") ACTOR actor TIME ts EVENT event "
            "WITH (time_unit = 'us', shards = 1)" % event
        ),
    )


def assert_paths(runtime, expected):
    """Check indexed and scanned answers against independently known events."""
    assert execute(runtime, PATHS) == expected
    assert execute(runtime, PATHS + " USING SCAN") == expected


def assert_fresh_process(config):
    """Check persisted index contents with an empty process-wide block cache."""
    result = subprocess.run(
        [
            os.environ["VIRTUAL_ENV"] + "/bin/python3",
            "-c",
            CHILD_QUERY,
            str(config),
            str(Path(fedq.__file__).parent),
            PATHS,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 2, result.stdout
    for line in lines:
        assert json.loads(line) == EXPECTED_NEW


def test_same_name_recreation_is_correct_in_warm_and_fresh_processes(tmp_path):
    """A recreated dataset cannot inherit data or indexes from its deleted owner."""
    config = tmp_path / "config.yaml"
    config.write_text("datasources: {}\n")
    runtime = fedq.Runtime(str(config))
    peer = fedq.Runtime(str(config))
    create_dataset(runtime, changed=False)
    assert_paths(runtime, EXPECTED_OLD)
    assert_paths(peer, EXPECTED_OLD)
    execute(runtime, "DROP EVENT DATASET recreated")
    create_dataset(runtime, changed=True)
    assert_paths(runtime, EXPECTED_NEW)
    assert_paths(peer, EXPECTED_NEW)
    assert_fresh_process(config)
