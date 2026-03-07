"""
worker.py

GHA worker loop — runs inside a GitHub Actions runner VM.

Lifecycle:
  1. Connect to S3 queue.
  2. Pull a task (step-affine: prefer same step as last task).
  3. Set up environment (install requirements via pip; GHA caches by env_id).
  4. Download code package from S3.
  5. Execute: python flow.py step {step_name} ... (as reconstructed from metaflow_args).
  6. Mark task done / failed; unblock dependents.
  7. Periodically reclaim stale claimed tasks.
  8. Exit after max_idle_seconds with no work.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import traceback

from .aws_client import make_s3_client
from .s3_queue_client import S3QueueClient

# How often (in tasks processed) to check for stale claimed tasks
_RECLAIM_EVERY_N = 10
# How long a task must be claimed before it's considered stale
_STALE_AFTER = 3600  # 1 hour
# How often to flush accumulated log lines to S3 (seconds)
_LOG_FLUSH_INTERVAL = 5.0
# Local cache dir for downloaded code packages (persists across tasks in same job)
_PKG_CACHE_DIR = "/tmp/mf-pkg-cache"


def run_worker(
    run_id: str,
    worker_id: str,
    max_idle_seconds: int = 300,
) -> None:
    s3 = make_s3_client()
    client = S3QueueClient.from_env(s3)

    preferred_step: str | None = None
    tasks_processed = 0
    idle_since: float | None = None

    print(f"[worker:{worker_id}] Started. run_id={run_id}", flush=True)

    while True:
        # Periodic stale reclaim
        if tasks_processed > 0 and tasks_processed % _RECLAIM_EVERY_N == 0:
            reclaimed = client.reclaim_stale(run_id, stale_after_seconds=_STALE_AFTER)
            if reclaimed:
                print(f"[worker:{worker_id}] Reclaimed {reclaimed} stale task(s).", flush=True)

        task = client.claim_task(run_id, worker_id, preferred_step=preferred_step)

        if task is None:
            if idle_since is None:
                idle_since = time.monotonic()
                print(f"[worker:{worker_id}] Queue empty, waiting...", flush=True)
            elif time.monotonic() - idle_since >= max_idle_seconds:
                print(
                    f"[worker:{worker_id}] No tasks for {max_idle_seconds}s. Exiting.",
                    flush=True,
                )
                break
            time.sleep(5)
            continue

        idle_since = None
        task_id = task["task_id"]
        step_name = task["step_name"]
        preferred_step = step_name  # step-affine: keep preference
        tasks_processed += 1

        print(
            f"[worker:{worker_id}] Claimed task {task_id} (step={step_name}, "
            f"attempt={task['attempt']}).",
            flush=True,
        )

        try:
            _execute_task(task, worker_id, client, run_id)
            client.complete_task(run_id, task_id)
            print(f"[worker:{worker_id}] Task {task_id} done.", flush=True)
        except Exception as exc:
            error_msg = traceback.format_exc()
            print(
                f"[worker:{worker_id}] Task {task_id} failed:\n{error_msg}",
                flush=True,
            )
            client.fail_task(
                run_id=run_id,
                task_id=task_id,
                error=str(exc),
                attempt=task["attempt"],
                max_retries=task["max_retries"],
            )
            preferred_step = None  # reset preference after failure


def _execute_task(task: dict, worker_id: str, client: "S3QueueClient", run_id: str) -> None:
    """
    Set up environment and run the Metaflow step command for this task.
    Streams stdout/stderr to both GHA job logs (via print) and S3.
    """
    task_id = task["task_id"]

    with tempfile.TemporaryDirectory(prefix="mf-gha-task-") as workdir:
        # 1. Download and extract code package (cached by package_sha)
        _fetch_code_package(task["package_url"], task.get("package_sha", ""), workdir)

        # 2. Install requirements
        _setup_environment(workdir, task.get("env_id"))

        # 3. Build and run the metaflow step command, streaming output
        cmd = _build_step_command(task, workdir)
        print(f"[worker:{worker_id}] Running: {' '.join(cmd)}", flush=True)

        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={**os.environ, "METAFLOW_GHA_WORKER_ID": worker_id},
        )

        log_lines: list[str] = []
        last_flush = time.monotonic()

        def _flush_log():
            try:
                client.write_task_log(run_id, task_id, "\n".join(log_lines))
            except Exception:
                pass

        for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
            print(line, flush=True)  # visible in GHA job UI
            log_lines.append(line)
            if time.monotonic() - last_flush >= _LOG_FLUSH_INTERVAL:
                _flush_log()
                last_flush = time.monotonic()

        _flush_log()  # final flush
        proc.wait()

        if proc.returncode != 0:
            raise RuntimeError(
                f"Metaflow step exited with code {proc.returncode}"
            )


def _fetch_code_package(package_url: str, package_sha: str, workdir: str) -> None:
    """
    Download and extract the code package tarball from S3.
    Caches the tarball in _PKG_CACHE_DIR by package_sha so subsequent tasks
    on the same worker (same GHA job) skip the S3 download.
    """
    import tarfile

    os.makedirs(_PKG_CACHE_DIR, exist_ok=True)

    if package_sha:
        cached_path = os.path.join(_PKG_CACHE_DIR, f"{package_sha}.tar.gz")
    else:
        # No sha — can't cache safely; use a temp path
        cached_path = os.path.join(workdir, "code.tar.gz")

    if not os.path.exists(cached_path):
        s3 = make_s3_client()
        url = package_url[len("s3://"):]
        bucket, _, key = url.partition("/")
        # Download to a temp path first, then rename atomically to avoid
        # partial files being read by concurrent tasks
        tmp_path = cached_path + f".{os.getpid()}.tmp"
        try:
            s3.download_file(bucket, key, tmp_path)
            os.rename(tmp_path, cached_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    with tarfile.open(cached_path, "r:gz") as tar:
        tar.extractall(workdir)


def _setup_environment(workdir: str, env_id: str | None) -> None:
    """
    Install Python dependencies if a requirements.txt is present.
    env_id is used by the GHA cache action (keyed in worker.yml), not here.
    """
    req_path = os.path.join(workdir, "requirements.txt")
    if os.path.exists(req_path):
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", req_path, "-q"],
            check=True,
        )


def _build_step_command(task: dict, workdir: str) -> list[str]:
    """
    Reconstruct the `python flow.py step` command for this task.
    All fields come from the task definition written by `gha step`.
    """
    flow_file = task.get("flow_file") or f"{task['flow_name']}.py"

    cmd = [
        sys.executable,
        flow_file,
        "--datastore=s3",
        "--no-pylint",
        "step",
        task["step_name"],
        f"--run-id={task['run_id']}",
        f"--task-id={task['task_id']}",
        f"--retry-count={task['attempt']}",
        f"--max-user-code-retries={task.get('max_user_code_retries', 0)}",
    ]

    if task.get("input_paths"):
        cmd.append(f"--input-paths={task['input_paths']}")

    if task.get("split_index") is not None:
        cmd.append(f"--split-index={task['split_index']}")

    if task.get("namespace"):
        cmd.append(f"--namespace={task['namespace']}")

    if task.get("ubf_context"):
        cmd.append(f"--ubf-context={task['ubf_context']}")

    for tag in task.get("tag", []):
        cmd.append(f"--tag={tag}")

    return cmd
