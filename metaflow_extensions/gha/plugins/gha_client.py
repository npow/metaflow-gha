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

import os
import subprocess

_GHA_WORKER_REPO = os.environ.get(
    "METAFLOW_GHA_WORKER_REPO", "npow/metaflow-gha"
)
_GHA_WORKER_WORKFLOW = os.environ.get(
    "METAFLOW_GHA_WORKER_WORKFLOW", "worker.yml"
)
_GHA_WORKER_REF = os.environ.get(
    "METAFLOW_GHA_WORKER_REF", "main"
)
# Workflow file name in the USER's repo that calls the reusable worker workflow
_GHA_CALLER_WORKFLOW = os.environ.get(
    "METAFLOW_GHA_CALLER_WORKFLOW", "metaflow-gha.yml"
)
_GHA_USER_REPO = os.environ.get("METAFLOW_GHA_USER_REPO", "")
_GHA_DISPATCH_REF = os.environ.get("METAFLOW_GHA_DISPATCH_REF", "")


class GHAClientError(Exception):
    pass


class GHAClient:
    def __init__(
        self,
        user_repo: str,
        worker_repo: str,
        worker_workflow: str,
        caller_workflow: str = "metaflow-gha.yml",
        worker_ref: str = "main",
        dispatch_ref: str = "",
    ):
        self.user_repo = user_repo            # e.g. "myorg/myrepo"
        self.worker_repo = worker_repo        # e.g. "npow/metaflow-gha"
        self.worker_workflow = worker_workflow  # e.g. "worker.yml" in worker_repo
        self.caller_workflow = caller_workflow  # e.g. "metaflow-gha.yml" in user_repo
        self.worker_ref = worker_ref
        self.dispatch_ref = dispatch_ref

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
            worker_ref=_GHA_WORKER_REF,
            dispatch_ref=_GHA_DISPATCH_REF,
        )

    def ensure_workers(self, run_id: str, n_workers: int = 20, s3_client=None) -> None:
        """
        Dispatch n_workers GHA jobs for this run.
        Idempotent: uses an S3 conditional write sentinel so only the first
        `gha step` call in a run actually dispatches workers; subsequent calls
        (for later tasks in the same run) are no-ops.
        """
        from .aws_client import make_s3_client
        from .s3_queue_client import S3QueueClient

        self._sync_worker_env_to_repo()

        s3 = s3_client or make_s3_client()
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

    def _sync_worker_env_to_repo(self) -> None:
        """
        Proxy selected local env vars into GitHub Actions configuration.

        Secrets:
          - AWS_ACCESS_KEY_ID
          - AWS_SECRET_ACCESS_KEY
          - AWS_SESSION_TOKEN
          - METAFLOW_SERVICE_AUTH_KEY
        Variables:
          - AWS_ENDPOINT_URL_S3
          - METAFLOW_S3_ENDPOINT_URL
        """
        secret_keys = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "METAFLOW_SERVICE_AUTH_KEY",
        ]
        variable_keys = [
            "AWS_ENDPOINT_URL_S3",
            "METAFLOW_S3_ENDPOINT_URL",
            "METAFLOW_SERVICE_URL",
            "METAFLOW_DEFAULT_METADATA",
        ]

        for key in secret_keys:
            value = os.environ.get(key)
            if not value:
                continue
            result = subprocess.run(
                ["gh", "secret", "set", key, "--repo", self.user_repo, "--body", value],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise GHAClientError(
                    f"Failed to set repo secret {key} on {self.user_repo}:\n{result.stderr}"
                )

        for key in variable_keys:
            value = os.environ.get(key)
            if not value:
                continue
            result = subprocess.run(
                ["gh", "variable", "set", key, "--repo", self.user_repo, "--body", value],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise GHAClientError(
                    f"Failed to set repo variable {key} on {self.user_repo}:\n{result.stderr}"
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
            "worker_ref": self.worker_ref,
        }
        field_args = []
        for k, v in inputs.items():
            field_args += ["-f", f"{k}={v}"]

        cmd = [
            "gh", "workflow", "run", self.caller_workflow,
            "--repo", self.user_repo,
        ] + field_args
        if self.dispatch_ref:
            cmd += ["--ref", self.dispatch_ref]

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
        caller_yml = _CALLER_WORKFLOW_TEMPLATE.format(
            worker_repo=self.worker_repo,
            worker_workflow=self.worker_workflow,
            worker_ref=self.worker_ref,
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
      worker_ref:
        required: false
        default: "main"
        type: string

jobs:
  worker:
    uses: {worker_repo}/.github/workflows/{worker_workflow}@{worker_ref}
    with:
      run_id: ${{{{ inputs.run_id }}}}
      worker_index: ${{{{ inputs.worker_index }}}}
      s3_root: ${{{{ inputs.s3_root }}}}
      worker_ref: ${{{{ inputs.worker_ref }}}}
    secrets: inherit
"""
