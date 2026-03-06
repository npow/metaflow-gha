"""
gha_cli.py

`gha` CLI subcommand group.

Subcommands:
  gha step   — called by Metaflow runtime on the orchestrating machine;
               pushes this task to the S3 queue and waits for completion.
  gha worker — called inside the GHA runner VM; pulls and executes tasks.
"""
from __future__ import annotations

import os
import sys
import time
import uuid

from metaflow._vendor import click


@click.group()
def cli():
    """Root Metaflow extension CLI group."""
    pass


@cli.group(name="gha")
def gha():
    """GitHub Actions compute backend for Metaflow."""
    pass


# ---------------------------------------------------------------------------
# `gha step`  (runs on the user's machine / Metaflow orchestrator)
# ---------------------------------------------------------------------------

@gha.command(help="Submit a step task to the GHA S3 queue and wait for completion.")
@click.argument("step_name")
@click.argument("package_sha")
@click.argument("package_url")
# Metaflow runtime injects these via cli_args.command_options (from default_args):
@click.option("--flow-name", required=True, help="Metaflow flow class name.")
@click.option("--run-id", required=True, help="Metaflow run ID.")
@click.option("--task-id", required=True, help="Metaflow task ID.")
@click.option("--input-paths", default=None, help="Compressed input pathspecs.")
@click.option("--split-index", default=None, help="Foreach split index.")
@click.option("--retry-count", default=0, type=int, help="Current retry attempt.")
@click.option("--max-user-code-retries", default=0, type=int)
@click.option("--tag", multiple=True, default=None)
@click.option("--namespace", default=None)
@click.option("--ubf-context", default=None)
# GHA-specific options from the decorator:
@click.option("--workers", default=20, show_default=True)
@click.option("--timeout", default=21600, show_default=True)
@click.option("--max-retries", default=2, show_default=True)
@click.pass_context
def step(
    ctx,
    step_name,
    package_sha,
    package_url,
    flow_name,
    run_id,
    task_id,
    input_paths,
    split_index,
    retry_count,
    max_user_code_retries,
    tag,
    namespace,
    ubf_context,
    workers,
    timeout,
    max_retries,
):
    """Push a Metaflow step task to the S3 queue and block until done or failed."""
    from .s3_queue_client import S3QueueClient
    from .aws_client import make_s3_client

    s3 = make_s3_client()
    client = S3QueueClient.from_env(s3)

    pathspec = f"{flow_name}/{run_id}/{step_name}/{task_id}"

    # Derive parent task IDs from input_paths (run_id/step_name/task_id/attempt format)
    parent_task_ids: list[str] = []
    if input_paths:
        for p in input_paths.split(","):
            parts = p.split("/")
            if len(parts) >= 3:
                parent_task_ids.append(parts[2])

    task = {
        "task_id": task_id,
        "run_id": run_id,
        "step_name": step_name,
        "flow_name": flow_name,
        "pathspec": pathspec,
        "input_paths": input_paths,
        "split_index": split_index,
        "parent_task_ids": parent_task_ids,
        "attempt": retry_count,
        "max_retries": max_retries,
        "max_user_code_retries": max_user_code_retries,
        "timeout_seconds": timeout,
        "package_url": package_url,
        "package_sha": package_sha,
        "tag": list(tag) if tag else [],
        "namespace": namespace,
        "ubf_context": ubf_context,
    }

    click.echo(f"[gha] Pushing task {task_id} (step={step_name}) to S3 queue.")
    client.push_task(run_id, task)

    # Ensure workers are running — idempotent via S3 sentinel
    from .gha_client import GHAClient
    gha = GHAClient.from_env()
    gha.ensure_workers(run_id=run_id, n_workers=workers, s3_client=s3)

    # Poll for completion, streaming logs and reclaiming stale tasks
    _wait_for_task(client, run_id, task_id, timeout)


_RECLAIM_INTERVAL = 60.0   # seconds between reclaim_stale calls
_LOG_POLL_INTERVAL = 5.0   # seconds between log/state polls


def _wait_for_task(client, run_id: str, task_id: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    log_lines_seen = 0
    last_reclaim = time.monotonic()

    click.echo(f"[gha] Waiting for task {task_id}...")

    while time.monotonic() < deadline:
        # Reclaim stale claimed tasks periodically — handles crashed workers
        if time.monotonic() - last_reclaim >= _RECLAIM_INTERVAL:
            try:
                reclaimed = client.reclaim_stale(run_id)
                if reclaimed:
                    click.echo(f"[gha] Reclaimed {reclaimed} stale task(s).")
            except Exception:
                pass
            last_reclaim = time.monotonic()

        # Stream new log lines from S3
        try:
            log_content = client.read_task_log(run_id, task_id)
            if log_content:
                lines = log_content.splitlines()
                for line in lines[log_lines_seen:]:
                    click.echo(line)
                log_lines_seen = len(lines)
        except Exception:
            pass

        state = client.get_task_state(run_id, task_id)
        if state == "done":
            click.echo(f"[gha] Task {task_id} completed successfully.")
            return
        if state == "failed":
            raise click.ClickException(f"GHA task {task_id} failed permanently.")

        time.sleep(_LOG_POLL_INTERVAL)

    raise click.ClickException(
        f"GHA task {task_id} timed out after {timeout}s waiting for completion."
    )


# ---------------------------------------------------------------------------
# `gha worker`  (runs inside the GHA runner VM)
# ---------------------------------------------------------------------------

@gha.command(help="Write the caller workflow to .github/workflows/metaflow-gha.yml and commit it.")
def inject():
    """
    One-time setup: write a thin caller workflow to your repo so that
    `python flow.py run --with=gha` can dispatch workers without you committing
    metaflow-gha code directly.

    Run once, commit, and push:
        python flow.py gha inject
        git add .github/workflows/metaflow-gha.yml
        git commit -m "chore: add metaflow-gha caller workflow"
        git push
    """
    from .gha_client import GHAClient
    gha = GHAClient.from_env()
    path = gha.inject_caller_workflow()
    click.echo(f"[gha] Wrote caller workflow to {path}")
    click.echo("[gha] Commit and push this file, then you're ready to use @gha.")


@gha.command(help="Start a GHA worker that pulls and executes tasks from the S3 queue.")
@click.option("--run-id", required=True, help="Metaflow run ID to process tasks for.")
@click.option("--worker-id", default=None, help="Unique worker identifier (auto-generated if omitted).")
@click.option("--max-idle-seconds", default=300, show_default=True,
              help="Seconds to wait for new tasks before exiting.")
def worker(run_id, worker_id, max_idle_seconds):
    """Pull tasks from the S3 queue and execute them until idle or run completes."""
    from .worker import run_worker

    if worker_id is None:
        worker_id = f"gha-worker-{uuid.uuid4().hex[:8]}"

    click.echo(f"[gha-worker] Starting worker {worker_id} for run {run_id}.")
    run_worker(run_id=run_id, worker_id=worker_id, max_idle_seconds=max_idle_seconds)


if __name__ == "__main__":
    cli()
