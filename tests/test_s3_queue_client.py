"""
Tests for S3QueueClient wrapper using moto.
Verifies the thin delegation layer works end-to-end.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Ensure real metaflow_coordinator.s3_queue is loaded (not the stub)
# See test_s3_queue.py for the bootstrap pattern.
# ---------------------------------------------------------------------------
import importlib.util
import json
import pathlib
import sys

import boto3
import pytest

_S3_QUEUE_PATH = pathlib.Path(
    "/Users/npow/code/metaflow-coordinator/metaflow_coordinator/s3_queue.py"
)

_HAS_REAL_S3_QUEUE = False
if _S3_QUEUE_PATH.exists():
    try:
        _spec = importlib.util.spec_from_file_location("_s3q_for_client", _S3_QUEUE_PATH)
        _s3q = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_s3q)
        # Inject as the module that s3_queue_client imports from
        sys.modules["metaflow_coordinator.s3_queue"] = _s3q
        _HAS_REAL_S3_QUEUE = True
    except Exception:
        pass

pytestmark = pytest.mark.skipif(
    not _HAS_REAL_S3_QUEUE, reason="metaflow_coordinator.s3_queue source not available"
)

try:
    from moto import mock_aws

    _HAS_MOTO = True
except ImportError:
    _HAS_MOTO = False

pytestmark = [
    pytest.mark.skipif(not _HAS_REAL_S3_QUEUE, reason="s3_queue source not available"),
    pytest.mark.skipif(not _HAS_MOTO, reason="moto not installed"),
]

# Import after patching sys.modules
from metaflow_extensions.gha.plugins.s3_queue_client import S3QueueClient  # noqa: E402

BUCKET = "test-bucket"
PREFIX = "mf"
RUN_ID = "999"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def queue(s3):
    return S3QueueClient(s3=s3, bucket=BUCKET, prefix=PREFIX)


def _make_task(task_id, step="start"):
    return {
        "task_id": task_id,
        "run_id": RUN_ID,
        "step_name": step,
        "flow_name": "TestFlow",
        "flow_file": "flow.py",
        "pathspec": f"TestFlow/{RUN_ID}/{step}/{task_id}",
        "input_paths": None,
        "parent_task_ids": [],
        "attempt": 0,
        "max_retries": 2,
        "timeout_seconds": 600,
        "package_url": "s3://test-bucket/pkg.tar.gz",
        "package_sha": "abc",
        "tag": [],
        "namespace": None,
        "ubf_context": None,
    }


# ---------------------------------------------------------------------------
# push_task / claim_task
# ---------------------------------------------------------------------------


def test_push_and_claim(queue):
    task = _make_task("t1")
    queue.push_task(RUN_ID, task)
    result = queue.claim_task(RUN_ID, "w1")
    assert result is not None
    assert result["task_id"] == "t1"


def test_claim_empty_returns_none(queue):
    assert queue.claim_task(RUN_ID, "w1") is None


def test_claim_with_preferred_step(queue):
    queue.push_task(RUN_ID, _make_task("ta", step="a"))
    queue.push_task(RUN_ID, _make_task("tb", step="b"))
    result = queue.claim_task(RUN_ID, "w1", preferred_step="b")
    assert result["step_name"] == "b"


# ---------------------------------------------------------------------------
# complete_task / fail_task
# ---------------------------------------------------------------------------


def test_complete_task(queue):
    queue.push_task(RUN_ID, _make_task("t1"))
    queue.claim_task(RUN_ID, "w1")
    queue.complete_task(RUN_ID, "t1")
    pending = queue.list_pending(RUN_ID)
    assert "t1" in pending["done"]


def test_fail_task_with_retries(queue):
    queue.push_task(RUN_ID, _make_task("t1"))
    queue.claim_task(RUN_ID, "w1")
    queue.fail_task(RUN_ID, "t1", "error!", attempt=0, max_retries=2)
    pending = queue.list_pending(RUN_ID)
    assert "t1-retry1" in pending["ready"]


def test_fail_task_no_retries(queue):
    task = {**_make_task("t1"), "max_retries": 0}
    queue.push_task(RUN_ID, task)
    queue.claim_task(RUN_ID, "w1")
    queue.fail_task(RUN_ID, "t1", "fatal", attempt=0, max_retries=0)
    pending = queue.list_pending(RUN_ID)
    assert "t1" in pending["failed"]


# ---------------------------------------------------------------------------
# reclaim_stale
# ---------------------------------------------------------------------------


def test_reclaim_stale(queue, s3):
    import time

    queue.push_task(RUN_ID, _make_task("t1"))
    queue.claim_task(RUN_ID, "w1")
    # Manually make the claim look stale
    from metaflow_coordinator.s3_queue import _s3_root

    claimed_key = f"{_s3_root(BUCKET, PREFIX, RUN_ID)}/claimed/t1"
    s3.put_object(
        Bucket=BUCKET,
        Key=claimed_key,
        Body=json.dumps({"worker_id": "w1", "claimed_at": time.time() - 7200}),
        ContentType="application/json",
    )
    n = queue.reclaim_stale(RUN_ID, stale_after_seconds=60)
    assert n == 1


# ---------------------------------------------------------------------------
# mark_workers_dispatched
# ---------------------------------------------------------------------------


def test_mark_workers_dispatched(queue):
    assert queue.mark_workers_dispatched(RUN_ID, 5) is True
    assert queue.mark_workers_dispatched(RUN_ID, 5) is False


# ---------------------------------------------------------------------------
# write / read log
# ---------------------------------------------------------------------------


def test_write_read_log(queue):
    queue.write_task_log(RUN_ID, "t1", "line1\nline2")
    assert queue.read_task_log(RUN_ID, "t1") == "line1\nline2"


def test_read_missing_log(queue):
    assert queue.read_task_log(RUN_ID, "missing") is None


# ---------------------------------------------------------------------------
# from_env
# ---------------------------------------------------------------------------


def test_from_env(monkeypatch, s3):
    monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_S3", f"s3://{BUCKET}/{PREFIX}")
    client = S3QueueClient.from_env(s3)
    assert client.bucket == BUCKET
    assert client.prefix == PREFIX


# ---------------------------------------------------------------------------
# get_task_state
# ---------------------------------------------------------------------------


def test_get_task_state_done(queue):
    queue.push_task(RUN_ID, _make_task("t1"))
    queue.claim_task(RUN_ID, "w1")
    queue.complete_task(RUN_ID, "t1")
    assert queue.get_task_state(RUN_ID, "t1") == "done"


def test_get_task_state_failed(queue):
    queue.push_task(RUN_ID, {**_make_task("t1"), "max_retries": 0})
    queue.claim_task(RUN_ID, "w1")
    queue.fail_task(RUN_ID, "t1", "err", attempt=0, max_retries=0)
    assert queue.get_task_state(RUN_ID, "t1") == "failed"


def test_get_task_state_claimed(queue):
    queue.push_task(RUN_ID, _make_task("t1"))
    queue.claim_task(RUN_ID, "w1")
    assert queue.get_task_state(RUN_ID, "t1") == "claimed"


def test_get_task_state_waiting(queue):
    queue.push_task(RUN_ID, _make_task("parent"))
    queue.push_task(RUN_ID, {**_make_task("child"), "parent_task_ids": ["parent"]})
    assert queue.get_task_state(RUN_ID, "child") == "waiting"


def test_get_task_state_ready(queue):
    queue.push_task(RUN_ID, _make_task("t1"))
    assert queue.get_task_state(RUN_ID, "t1") == "ready"
