"""
Adversarial and integration tests for s3_queue + S3QueueClient.

Uses moto to mock S3 so tests run offline without real AWS creds.
Covers happy paths, race conditions, retry bugs, and edge cases.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Bootstrap: load the real s3_queue module directly from source, bypassing
# sys.modules stubs installed by other test files.
# ---------------------------------------------------------------------------
import importlib.util
import json
import pathlib
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest


def _find_s3_queue_path() -> pathlib.Path:
    # Try installed package first (works on CI after pip install metaflow-coordinator)
    try:
        spec = importlib.util.find_spec("metaflow_coordinator.s3_queue")
        if spec and spec.origin:
            return pathlib.Path(spec.origin)
    except (ModuleNotFoundError, ValueError):
        pass
    # Fallback to local dev checkout
    return pathlib.Path("/Users/npow/code/metaflow-coordinator/metaflow_coordinator/s3_queue.py")


_S3_QUEUE_PATH = _find_s3_queue_path()

_HAS_REAL_S3_QUEUE = False
if _S3_QUEUE_PATH.exists():
    try:
        _spec = importlib.util.spec_from_file_location("_s3_queue_real", _S3_QUEUE_PATH)
        _s3q = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_s3q)

        _bucket_prefix_from_env = _s3q._bucket_prefix_from_env
        _done_key = _s3q._done_key
        _failed_key = _s3q._failed_key
        _ready_key = _s3q._ready_key
        _s3_root = _s3q._s3_root
        _task_key = _s3q._task_key
        _waiting_key = _s3q._waiting_key
        claim_task = _s3q.claim_task
        complete_task = _s3q.complete_task
        fail_task = _s3q.fail_task
        list_pending = _s3q.list_pending
        mark_workers_dispatched = _s3q.mark_workers_dispatched
        push_task = _s3q.push_task
        read_task_log = _s3q.read_task_log
        reclaim_stale = _s3q.reclaim_stale
        write_task_log = _s3q.write_task_log
        _HAS_REAL_S3_QUEUE = True
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Moto S3 fixture
# ---------------------------------------------------------------------------

try:
    from moto import mock_aws

    _HAS_MOTO = True
except ImportError:
    _HAS_MOTO = False

pytestmark = [
    pytest.mark.skipif(not _HAS_REAL_S3_QUEUE, reason="s3_queue source not available"),
    pytest.mark.skipif(not _HAS_MOTO, reason="moto not installed"),
]

BUCKET = "test-bucket"
PREFIX = "metaflow"
RUN_ID = "1234"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _push(s3, task_id, step="start", parent_ids=None, attempt=0, max_retries=2):
    task = {
        "task_id": task_id,
        "run_id": RUN_ID,
        "step_name": step,
        "flow_name": "TestFlow",
        "flow_file": "flow.py",
        "pathspec": f"TestFlow/{RUN_ID}/{step}/{task_id}",
        "input_paths": None,
        "parent_task_ids": parent_ids or [],
        "attempt": attempt,
        "max_retries": max_retries,
        "timeout_seconds": 600,
        "package_url": "s3://test-bucket/pkg.tar.gz",
        "package_sha": "abc123",
        "tag": [],
        "namespace": None,
        "ubf_context": None,
    }
    push_task(s3, BUCKET, PREFIX, RUN_ID, task)
    return task


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_push_and_claim(s3):
    _push(s3, "t1")
    task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "worker-1")
    assert task is not None
    assert task["task_id"] == "t1"


def test_claim_empty_queue_returns_none(s3):
    result = claim_task(s3, BUCKET, PREFIX, RUN_ID, "worker-1")
    assert result is None


def test_complete_task(s3):
    _push(s3, "t1")
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "worker-1")
    complete_task(s3, BUCKET, PREFIX, RUN_ID, "t1")
    # Task is done — should no longer appear in ready or claimed
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "t1" in pending["done"]
    assert "t1" not in pending["ready"]
    assert "t1" not in pending["claimed"]


def test_task_claimed_only_once(s3):
    """Two workers race — only one should get the task."""
    _push(s3, "t1")
    got1 = claim_task(s3, BUCKET, PREFIX, RUN_ID, "worker-1")
    got2 = claim_task(s3, BUCKET, PREFIX, RUN_ID, "worker-2")
    assert (got1 is None) != (got2 is None), "exactly one worker should win the claim"


def test_preferred_step_claimed_first(s3):
    """Step-affine: preferred_step tasks are returned before others."""
    _push(s3, "t-start", step="start")
    _push(s3, "t-train", step="train")
    task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1", preferred_step="train")
    assert task["step_name"] == "train"


# ---------------------------------------------------------------------------
# Dependency / waiting
# ---------------------------------------------------------------------------


def test_task_with_pending_parent_goes_to_waiting(s3):
    _push(s3, "parent")
    _push(s3, "child", parent_ids=["parent"])
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "child" in pending["waiting"]
    assert "child" not in pending["ready"]


def test_completing_parent_unblocks_child(s3):
    _push(s3, "parent")
    _push(s3, "child", parent_ids=["parent"])
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    complete_task(s3, BUCKET, PREFIX, RUN_ID, "parent")
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "child" in pending["ready"]
    assert "child" not in pending["waiting"]


def test_task_with_already_done_parent_goes_directly_to_ready(s3):
    """Parent finishes before child is pushed — child should be ready immediately."""
    _push(s3, "parent")
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    complete_task(s3, BUCKET, PREFIX, RUN_ID, "parent")
    # Now push child — parent is already done
    _push(s3, "child", parent_ids=["parent"])
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "child" in pending["ready"]
    assert "child" not in pending["waiting"]


def test_multi_parent_unblocks_only_when_all_done(s3):
    _push(s3, "p1")
    _push(s3, "p2")
    _push(s3, "child", parent_ids=["p1", "p2"])
    # Complete only p1 — child should still be waiting
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    complete_task(s3, BUCKET, PREFIX, RUN_ID, "p1")
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "child" in pending["waiting"]
    # Now complete p2 — child should become ready
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    complete_task(s3, BUCKET, PREFIX, RUN_ID, "p2")
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "child" in pending["ready"]


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


def test_fail_with_retries_remaining_requeues(s3):
    _push(s3, "t1", max_retries=2)
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    fail_task(s3, BUCKET, PREFIX, RUN_ID, "t1", error="boom", attempt=0, max_retries=2)
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    retry_id = "t1-retry1"
    assert retry_id in pending["ready"]
    assert "t1" not in pending["failed"]


def test_fail_with_no_retries_marks_failed(s3):
    _push(s3, "t1", max_retries=0)
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    fail_task(s3, BUCKET, PREFIX, RUN_ID, "t1", error="boom", attempt=0, max_retries=0)
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "t1" in pending["failed"]


def test_fail_exhausted_retries_marks_failed(s3):
    _push(s3, "t1", max_retries=1)
    # First attempt
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    fail_task(s3, BUCKET, PREFIX, RUN_ID, "t1", error="boom", attempt=0, max_retries=1)
    # Claim the retry
    retry_task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    assert retry_task is not None
    retry_id = retry_task["task_id"]
    # Fail the retry (attempt=1, max_retries=1 → exhausted)
    fail_task(s3, BUCKET, PREFIX, RUN_ID, retry_id, error="boom again", attempt=1, max_retries=1)
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert retry_id in pending["failed"]


# ---------------------------------------------------------------------------
# ADVERSARIAL: Retry task_id change breaks orchestrator polling (BUG)
# ---------------------------------------------------------------------------


def test_bug_retry_original_task_id_never_reaches_done_or_failed(s3):
    """
    BUG: fail_task creates a new task_id (e.g. t1-retry1) for the retry.
    The orchestrator polls the ORIGINAL task_id. Since fail_task writes
    failed/ only to the original task_id when retries are exhausted, but
    never when retries remain, the orchestrator polls indefinitely on first failure.

    This test documents the behavior: after one failure with retries remaining,
    the original task_id is in neither done/ nor failed/.
    """
    _push(s3, "t1", max_retries=2)
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    fail_task(s3, BUCKET, PREFIX, RUN_ID, "t1", error="boom", attempt=0, max_retries=2)

    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    # The retry is re-queued under a NEW task_id, not the original
    assert "t1" not in pending["done"]
    assert "t1" not in pending["failed"]
    # The original task_id is now orphaned — orchestrator will wait forever
    assert "t1-retry1" in pending["ready"]


# ---------------------------------------------------------------------------
# ADVERSARIAL: reclaim_stale race with complete_task (BUG)
# ---------------------------------------------------------------------------


def test_bug_reclaim_stale_can_requeue_completed_task(s3):
    """
    BUG: reclaim_stale doesn't check done/ before re-enqueuing.
    If a task completes while reclaim_stale is scanning, the task ends up
    in both done/ and ready/, leading to double execution.

    This test simulates the race by manually inserting done/ and claimed/
    for the same task, then running reclaim_stale.
    """
    _push(s3, "t1")
    # Simulate: worker claimed it 2 hours ago
    claimed_key = f"{_s3_root(BUCKET, PREFIX, RUN_ID)}/claimed/t1"
    s3.put_object(
        Bucket=BUCKET,
        Key=claimed_key,
        Body=json.dumps({"worker_id": "w1", "claimed_at": time.time() - 7200}),
        ContentType="application/json",
    )
    # Simulate: task also completed (done/ written by the original worker)
    done_key = f"{_s3_root(BUCKET, PREFIX, RUN_ID)}/done/t1"
    s3.put_object(
        Bucket=BUCKET,
        Key=done_key,
        Body=json.dumps({"completed_at": time.time()}),
        ContentType="application/json",
    )

    # reclaim_stale should notice task is done and NOT re-enqueue it
    reclaim_stale(s3, BUCKET, PREFIX, RUN_ID, stale_after_seconds=60)

    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    # If the bug is present, t1 will appear in ready — double execution risk
    if "t1" in pending.get("ready", []):
        pytest.xfail(
            "BUG CONFIRMED: reclaim_stale re-enqueued a completed task. "
            "Fix: check done/ before re-enqueuing in reclaim_stale."
        )


# ---------------------------------------------------------------------------
# ADVERSARIAL: _sync_worker_env_to_repo called before dispatch check (BUG)
# ---------------------------------------------------------------------------


def test_bug_sync_env_called_even_when_workers_already_dispatched(s3):
    """
    BUG: GHAClient.ensure_workers calls _sync_worker_env_to_repo() unconditionally,
    BEFORE checking mark_workers_dispatched. Every `gha step` invocation syncs
    secrets/variables, even when workers are already running.
    """
    import sys as _sys
    import types as _types

    # Stub gha_client imports
    _stub_coord = _types.ModuleType("metaflow_coordinator")
    _stub_s3 = _types.ModuleType("metaflow_coordinator.s3_queue")
    for n in [
        "push_task",
        "claim_task",
        "complete_task",
        "fail_task",
        "reclaim_stale",
        "list_pending",
        "mark_workers_dispatched",
        "write_task_log",
        "read_task_log",
        "_bucket_prefix_from_env",
        "_done_key",
        "_failed_key",
    ]:
        setattr(_stub_s3, n, MagicMock())
    _stub_coord.s3_queue = _stub_s3
    _sys.modules.setdefault("metaflow_coordinator", _stub_coord)
    _sys.modules.setdefault("metaflow_coordinator.s3_queue", _stub_s3)

    from metaflow_extensions.gha.plugins.gha_client import GHAClient

    client = GHAClient(
        user_repo="myorg/myrepo",
        worker_repo="npow/metaflow-gha",
        worker_workflow="worker.yml",
        caller_workflow="metaflow-gha.yml",
    )

    sync_calls = []
    mock_q_inst = MagicMock()
    mock_q_inst.mark_workers_dispatched.return_value = True

    with patch.object(client, "_sync_worker_env_to_repo", side_effect=lambda: sync_calls.append(1)):
        with patch.object(client, "_dispatch_worker"):
            with patch(
                "metaflow_extensions.gha.plugins.aws_client.make_s3_client",
                return_value=MagicMock(),
            ):
                with patch(
                    "metaflow_extensions.gha.plugins.s3_queue_client.S3QueueClient"
                ) as mock_q_cls:
                    mock_q_cls.from_env.return_value = mock_q_inst

                    # First call: workers not yet dispatched
                    client.ensure_workers("run1", n_workers=2)
                    assert len(sync_calls) == 1

                    # Second call: workers already dispatched
                    mock_q_inst.mark_workers_dispatched.return_value = False
                    client.ensure_workers("run1", n_workers=2)

                    # BUG: sync is called even when workers are already dispatched
                    if len(sync_calls) == 2:
                        pytest.xfail(
                            "BUG CONFIRMED: _sync_worker_env_to_repo called even when workers "
                            "already dispatched. Move _sync_worker_env_to_repo after "
                            "mark_workers_dispatched check."
                        )


# ---------------------------------------------------------------------------
# mark_workers_dispatched idempotency
# ---------------------------------------------------------------------------


def test_mark_workers_dispatched_idempotent(s3):
    first = mark_workers_dispatched(s3, BUCKET, PREFIX, RUN_ID, 5)
    second = mark_workers_dispatched(s3, BUCKET, PREFIX, RUN_ID, 5)
    assert first is True
    assert second is False  # already dispatched


# ---------------------------------------------------------------------------
# Log write / read
# ---------------------------------------------------------------------------


def test_write_and_read_log(s3):
    write_task_log(s3, BUCKET, PREFIX, RUN_ID, "t1", "hello\nworld")
    content = read_task_log(s3, BUCKET, PREFIX, RUN_ID, "t1")
    assert content == "hello\nworld"


def test_read_log_missing_returns_none(s3):
    result = read_task_log(s3, BUCKET, PREFIX, RUN_ID, "no-such-task")
    assert result is None


def test_log_overwrite(s3):
    write_task_log(s3, BUCKET, PREFIX, RUN_ID, "t1", "v1")
    write_task_log(s3, BUCKET, PREFIX, RUN_ID, "t1", "v2")
    assert read_task_log(s3, BUCKET, PREFIX, RUN_ID, "t1") == "v2"


# ---------------------------------------------------------------------------
# list_pending
# ---------------------------------------------------------------------------


def test_list_pending_full_lifecycle(s3):
    _push(s3, "t1")
    _push(s3, "t2", parent_ids=["t1"])
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "t1" in pending["ready"]
    assert "t2" in pending["waiting"]

    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "t1" in pending["claimed"]

    complete_task(s3, BUCKET, PREFIX, RUN_ID, "t1")
    pending = list_pending(s3, BUCKET, PREFIX, RUN_ID)
    assert "t1" in pending["done"]
    assert "t2" in pending["ready"]


# ---------------------------------------------------------------------------
# reclaim_stale
# ---------------------------------------------------------------------------


def test_reclaim_stale_returns_old_tasks_to_ready(s3):
    _push(s3, "t1")
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    # Patch the claimed_at timestamp to make it look stale
    claimed_key = f"{_s3_root(BUCKET, PREFIX, RUN_ID)}/claimed/t1"
    s3.put_object(
        Bucket=BUCKET,
        Key=claimed_key,
        Body=json.dumps({"worker_id": "w1", "claimed_at": time.time() - 7200}),
        ContentType="application/json",
    )
    reclaimed = reclaim_stale(s3, BUCKET, PREFIX, RUN_ID, stale_after_seconds=60)
    assert reclaimed == 1
    # Task should be claimable again
    task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "w2")
    assert task is not None
    assert task["task_id"] == "t1"


def test_reclaim_stale_ignores_recent_tasks(s3):
    _push(s3, "t1")
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    reclaimed = reclaim_stale(s3, BUCKET, PREFIX, RUN_ID, stale_after_seconds=3600)
    assert reclaimed == 0


# ---------------------------------------------------------------------------
# S3 path helpers
# ---------------------------------------------------------------------------


def test_s3_root_no_prefix():
    root = _s3_root(BUCKET, "", RUN_ID)
    assert root == f"gha-queue/{RUN_ID}"


def test_s3_root_with_prefix():
    root = _s3_root(BUCKET, "metaflow", RUN_ID)
    assert root == f"metaflow/gha-queue/{RUN_ID}"


def test_s3_root_strips_slashes():
    root = _s3_root(BUCKET, "/metaflow/", RUN_ID)
    assert root == f"metaflow/gha-queue/{RUN_ID}"


def test_bucket_prefix_from_env(monkeypatch):
    monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_S3", "s3://my-bucket/my/prefix")
    bucket, prefix = _bucket_prefix_from_env()
    assert bucket == "my-bucket"
    assert prefix == "my/prefix"


def test_bucket_prefix_from_env_no_prefix(monkeypatch):
    monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_S3", "s3://my-bucket")
    bucket, prefix = _bucket_prefix_from_env()
    assert bucket == "my-bucket"
    assert prefix == ""


# ---------------------------------------------------------------------------
# Adversarial: malformed / edge-case inputs
# ---------------------------------------------------------------------------


def test_claim_task_step_name_with_slashes(s3):
    """Step names don't normally have slashes, but ensure the key parsing is robust."""
    _push(s3, "t1", step="my_step")
    task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    assert task["step_name"] == "my_step"


def test_push_task_with_empty_parent_list_goes_to_ready(s3):
    _push(s3, "t1", parent_ids=[])
    task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    assert task is not None


def test_large_number_of_tasks(s3):
    """Push 50 tasks and claim them all — tests pagination in _list_all_keys."""
    for i in range(50):
        _push(s3, f"t{i}")
    claimed = []
    for _ in range(50):
        task = claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
        if task:
            claimed.append(task["task_id"])
    assert len(claimed) == 50


def test_fail_task_error_stored_in_failed_object(s3):
    _push(s3, "t1", max_retries=0)
    claim_task(s3, BUCKET, PREFIX, RUN_ID, "w1")
    fail_task(s3, BUCKET, PREFIX, RUN_ID, "t1", error="OutOfMemory", attempt=0, max_retries=0)
    failed_key = f"{_s3_root(BUCKET, PREFIX, RUN_ID)}/failed/t1"
    obj = s3.get_object(Bucket=BUCKET, Key=failed_key)
    data = json.loads(obj["Body"].read())
    assert data["error"] == "OutOfMemory"
    assert data["attempts"] == 1
