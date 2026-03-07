"""
s3_queue_client.py

Thin wrapper around the s3_queue module from metaflow-coordinator,
providing a convenient object-oriented interface for gha_cli and worker.py.
"""

from __future__ import annotations

from typing import Any

from metaflow_coordinator.s3_queue import (
    _bucket_prefix_from_env,
    _done_key,
    _failed_key,
    claim_task,
    complete_task,
    fail_task,
    list_pending,
    mark_workers_dispatched,
    push_task,
    read_task_log,
    reclaim_stale,
    write_task_log,
)


class S3QueueClient:
    def __init__(self, s3: Any, bucket: str, prefix: str):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix

    @classmethod
    def from_env(cls, s3: Any) -> "S3QueueClient":
        bucket, prefix = _bucket_prefix_from_env()
        return cls(s3=s3, bucket=bucket, prefix=prefix)

    def push_task(self, run_id: str, task: dict) -> None:
        push_task(self.s3, self.bucket, self.prefix, run_id, task)

    def claim_task(
        self, run_id: str, worker_id: str, preferred_step: str | None = None
    ) -> dict | None:
        return claim_task(self.s3, self.bucket, self.prefix, run_id, worker_id, preferred_step)

    def complete_task(self, run_id: str, task_id: str) -> None:
        complete_task(self.s3, self.bucket, self.prefix, run_id, task_id)

    def fail_task(
        self,
        run_id: str,
        task_id: str,
        error: str,
        attempt: int,
        max_retries: int,
    ) -> None:
        fail_task(self.s3, self.bucket, self.prefix, run_id, task_id, error, attempt, max_retries)

    def reclaim_stale(self, run_id: str, stale_after_seconds: int = 3600) -> int:
        return reclaim_stale(self.s3, self.bucket, self.prefix, run_id, stale_after_seconds)

    def list_pending(self, run_id: str) -> dict[str, list[str]]:
        return list_pending(self.s3, self.bucket, self.prefix, run_id)

    def mark_workers_dispatched(self, run_id: str, n_workers: int) -> bool:
        return mark_workers_dispatched(self.s3, self.bucket, self.prefix, run_id, n_workers)

    def write_task_log(self, run_id: str, task_id: str, content: str) -> None:
        write_task_log(self.s3, self.bucket, self.prefix, run_id, task_id, content)

    def read_task_log(self, run_id: str, task_id: str) -> str | None:
        return read_task_log(self.s3, self.bucket, self.prefix, run_id, task_id)

    def get_task_state(self, run_id: str, task_id: str) -> str:
        """Returns 'done', 'failed', 'claimed', 'ready', or 'waiting'."""
        for state_key_fn, state_name in [
            (_done_key, "done"),
            (_failed_key, "failed"),
        ]:
            try:
                self.s3.head_object(
                    Bucket=self.bucket,
                    Key=state_key_fn(self.bucket, self.prefix, run_id, task_id),
                )
                return state_name
            except Exception:
                pass

        # Check claimed
        from metaflow_coordinator.s3_queue import _claimed_key, _waiting_key

        for state_key_fn, state_name in [
            (_claimed_key, "claimed"),
            (_waiting_key, "waiting"),
        ]:
            try:
                self.s3.head_object(
                    Bucket=self.bucket,
                    Key=state_key_fn(self.bucket, self.prefix, run_id, task_id),
                )
                return state_name
            except Exception:
                pass

        return "ready"
