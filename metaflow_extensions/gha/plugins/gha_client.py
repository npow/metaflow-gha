"""
gha_client.py

Wraps the `gh` CLI to dispatch GitHub Actions workflows for worker VMs.

Authentication: relies on `gh auth login` (OAuth) — no PAT required.
The `gh` binary must be available in PATH (pre-installed on GHA runners).

Worker dispatch:
  - Uses a reusable workflow in the metaflow-gha repo
    (.github/workflows/worker.yml) via `gh workflow run`.
  - The caller workflow (injected into the user's repo) just calls the
    reusable workflow with the run_id and S3 config as inputs.
"""
from __future__ import annotations

import json
import os
import subprocess
from typing import Any


_GHA_WORKER_REPO = os.environ.get(
    "METAFLOW_GHA_WORKER_REPO", "npow/metaflow-gha"
)
_GHA_WORKER_WORKFLOW = os.environ.get(
    "METAFLOW_GHA_WORKER_WORKFLOW", "worker.yml"
)
# Workflow file name in the USER's repo that calls the reusable worker workflow
_GHA_CALLER_WORKFLOW = os.environ.get(
    "METAFLOW_GHA_CALLER_WORKFLOW", "metaflow-gha.yml"
)
_GHA_USER_REPO = os.environ.get("METAFLOW_GHA_USER_REPO", "")


class GHAClientError(Exception):
    pass


class GHAClient:
    def __init__(self, user_repo: str, worker_repo: str, worker_workflow: str,
                 caller_workflow: str = "metaflow-gha.yml"):
        self.user_repo = user_repo            # e.g. "myorg/myrepo"
        self.worker_repo = worker_repo        # e.g. "npow/metaflow-gha"
        self.worker_workflow = worker_workflow  # e.g. "worker.yml" in worker_repo
        self.caller_workflow = caller_workflow  # e.g. "metaflow-gha.yml" in user_repo

    @classmethod
    def from_env(cls) -> "GHAClient":
        user_repo = _GHA_USER_REPO
        if not user_repo:
            # Try to infer from git remote
            try:
                result = subprocess.run(
                    ["git", "remote", "get-url", "origin"],
                    capture_output=True, text=True, check=True,
                )
                url = result.stdout.strip()
                # Parse github.com/{owner}/{repo} or git@github.com:{owner}/{repo}
                if "github.com" in url:
                    user_repo = url.split("github.com", 1)[1].lstrip("/:").rstrip(".git")
            except Exception:
                pass
        if not user_repo:
            raise GHAClientError(
                "Cannot determine GitHub repo. Set METAFLOW_GHA_USER_REPO "
                "(e.g. 'myorg/myrepo') or run from a git repository with a "
                "GitHub remote."
            )
        return cls(
            user_repo=user_repo,
            worker_repo=_GHA_WORKER_REPO,
            worker_workflow=_GHA_WORKER_WORKFLOW,
            caller_workflow=_GHA_CALLER_WORKFLOW,
        )

    def ensure_workers(self, run_id: str, n_workers: int = 20, s3_client=None) -> None:
        """
        Dispatch n_workers GHA jobs for this run.
        Idempotent: uses an S3 conditional write sentinel so only the first
        `gha step` call in a run actually dispatches workers; subsequent calls
        (for later tasks in the same run) are no-ops.
        """
        import boto3
        from .s3_queue_client import S3QueueClient

        s3 = s3_client or boto3.client("s3")
        client = S3QueueClient.from_env(s3)

        if not client.mark_workers_dispatched(run_id, n_workers):
            return  # already dispatched by an earlier task in this run

        s3_root = os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3", "")
        for i in range(n_workers):
            self._dispatch_worker(
                run_id=run_id,
                worker_index=i,
                s3_root=s3_root,
            )

    def _dispatch_worker(self, run_id: str, worker_index: int, s3_root: str) -> None:
        """
        Trigger one worker job via `gh workflow run` on the USER's repo.

        Dispatches the caller workflow (metaflow-gha.yml) which the user must
        have committed via `python flow.py gha inject`. The caller workflow
        in turn calls the reusable worker.yml in npow/metaflow-gha.

        Inputs passed as workflow dispatch inputs:
          - run_id: Metaflow run ID
          - worker_index: worker number (0-based)
          - s3_root: METAFLOW_DATASTORE_SYSROOT_S3 value
        """
        inputs = {
            "run_id": run_id,
            "worker_index": str(worker_index),
            "s3_root": s3_root,
        }
        field_args = []
        for k, v in inputs.items():
            field_args += ["-f", f"{k}={v}"]

        cmd = [
            "gh", "workflow", "run", self.caller_workflow,
            "--repo", self.user_repo,
        ] + field_args

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            stderr = result.stderr
            if "workflow not found" in stderr.lower() or "could not find" in stderr.lower():
                raise GHAClientError(
                    f"Caller workflow '{self.caller_workflow}' not found in {self.user_repo}.\n"
                    "Run `python flow.py gha inject`, commit the generated file, and push."
                )
            raise GHAClientError(
                f"Failed to dispatch worker {worker_index} for run {run_id}:\n{stderr}"
            )

    def inject_caller_workflow(self, target_repo: str | None = None) -> str:
        """
        Write a minimal caller workflow YAML to .github/workflows/metaflow-gha.yml
        in the target repo (defaults to self.user_repo). Returns the file path.

        The caller workflow calls the reusable worker.yml from the metaflow-gha
        repo, so no metaflow-gha code needs to be committed to the user's repo.
        """
        repo = target_repo or self.user_repo
        caller_yml = _CALLER_WORKFLOW_TEMPLATE.format(
            worker_repo=self.worker_repo,
            worker_workflow=self.worker_workflow,
        )
        path = ".github/workflows/metaflow-gha.yml"
        os.makedirs(".github/workflows", exist_ok=True)
        with open(path, "w") as f:
            f.write(caller_yml)
        return path


# ---------------------------------------------------------------------------
# Reusable caller workflow template
# ---------------------------------------------------------------------------

_CALLER_WORKFLOW_TEMPLATE = """\
# Auto-generated by metaflow-gha — do not edit manually.
# This workflow is a thin caller that delegates to the reusable worker
# in {worker_repo}. No metaflow-gha code needs to live in your repo.
name: Metaflow GHA Worker

on:
  workflow_dispatch:
    inputs:
      run_id:
        required: true
        type: string
      worker_index:
        required: true
        type: string
      s3_root:
        required: true
        type: string

jobs:
  worker:
    uses: {worker_repo}/.github/workflows/{worker_workflow}@main
    with:
      run_id: ${{{{ inputs.run_id }}}}
      worker_index: ${{{{ inputs.worker_index }}}}
      s3_root: ${{{{ inputs.s3_root }}}}
    secrets: inherit
"""
