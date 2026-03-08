# metaflow-gha

[![CI](https://github.com/npow/metaflow-gha/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-gha/actions/workflows/ci.yml)
[![E2E](https://github.com/npow/metaflow-gha/actions/workflows/e2e.yml/badge.svg)](https://github.com/npow/metaflow-gha/actions/workflows/e2e.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-gha)](https://pypi.org/project/metaflow-gha/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Run your Metaflow steps on free GitHub Actions VMs without changing your flow code.

## The problem

Running Metaflow on AWS Batch or Kubernetes requires cloud accounts, IAM roles, container registries, and billing. For personal projects, side work, or early prototyping, the setup cost is disproportionate to the compute need. GitHub Actions gives you 20 parallel Ubuntu VMs for free on public repos — but there's no way to use them as a Metaflow compute backend.

## Quick start

```bash
pip install metaflow-gha
```

One-time setup in your flow's repo:

```bash
python flow.py gha inject   # writes .github/workflows/metaflow-gha.yml
git add .github/workflows/metaflow-gha.yml
git commit -m "chore: add metaflow-gha caller workflow"
git push
```

Then run any flow on GHA:

```bash
python flow.py run --with=gha
```

## Prerequisites

- Python 3.10+
- `metaflow` with `--datastore=s3`
- An S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, etc.) reachable from GitHub Actions
- [`gh` CLI](https://cli.github.com/) authenticated with `gh auth login` (or `GH_TOKEN` set)
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) for your S3 bucket, configured as GitHub repo secrets

## Install

```bash
pip install metaflow-gha
```

## Usage

**Apply to the whole flow via CLI:**

```bash
python flow.py --datastore=s3 run --with=gha
```

**Apply to specific steps with the decorator:**

```python
from metaflow import FlowSpec, step, gha

class MyFlow(FlowSpec):
    @gha
    @step
    def heavy_step(self):
        # runs on a GitHub Actions VM
        self.result = expensive_computation()
        self.next(self.end)

    @step
    def end(self):
        print(self.result)

if __name__ == "__main__":
    MyFlow()
```

**Control parallelism, timeout, and retries:**

```python
@gha(workers=10, timeout=3600, max_retries=1)
@step
def train(self):
    ...
```

| Parameter | Default | Description |
|---|---|---|
| `workers` | `20` | Number of parallel GHA runner VMs to spin up. |
| `timeout` | `21600` | Max wall-clock seconds per task (GHA limit: 6 hours). |
| `max_retries` | `2` | Times to retry a failed task before marking it permanently failed. |

## How it works

When `@gha` is active:

1. Each step task is pushed to an **S3-backed queue** instead of running locally.
2. Up to `workers` GitHub Actions jobs are dispatched once per run via `gh workflow run`.
3. Each GHA worker pulls a task, downloads the code package from S3, installs `requirements.txt`, and runs `python flow.py step ...` — standard Metaflow execution.
4. Workers use **S3 conditional writes** (`IfNoneMatch: *`) for atomic task claiming — no races with 20 concurrent workers.
5. Task **logs appear in the Metaflow UI** via mflog (same mechanism as Batch and Kubernetes backends).
6. The `gha step` orchestrator polls for task completion and streams logs back in real time.

The queue lives entirely in your Metaflow S3 datastore — no extra infrastructure beyond an S3 bucket.

## Configuration

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `METAFLOW_DATASTORE_SYSROOT_S3` | — | **Required.** Your Metaflow S3 root (e.g. `s3://my-bucket/metaflow`). |
| `METAFLOW_GHA_USER_REPO` | inferred from `git remote` | GitHub repo to dispatch workers on (e.g. `myorg/myrepo`). |
| `METAFLOW_GHA_WORKER_REPO` | `npow/metaflow-gha` | Repo hosting the reusable worker workflow. |
| `METAFLOW_GHA_WORKER_WORKFLOW` | `worker.yml` | Reusable workflow filename in the worker repo. |
| `METAFLOW_GHA_WORKER_REF` | `main` | Git ref of the worker repo to check out (pin to a tag/SHA for stability). |
| `METAFLOW_GHA_CALLER_WORKFLOW` | `metaflow-gha.yml` | Workflow filename in your repo (written by `gha inject`). |
| `METAFLOW_GHA_DISPATCH_REF` | `""` | Branch/tag in your repo to dispatch the caller workflow on. |

### GitHub repo secrets

AWS credentials for the worker VMs are passed via repository secrets. Set them once with the `gh` CLI:

```bash
gh secret set AWS_ACCESS_KEY_ID     --body "$AWS_ACCESS_KEY_ID"
gh secret set AWS_SECRET_ACCESS_KEY --body "$AWS_SECRET_ACCESS_KEY"
# Optional: for S3-compatible endpoints (Cloudflare R2, MinIO, etc.)
gh variable set AWS_ENDPOINT_URL_S3 --body "https://your-endpoint"
gh variable set METAFLOW_DATASTORE_SYSROOT_S3 --body "s3://your-bucket/metaflow"
```

Or let `gha step` sync them automatically the first time you run (when `gh` is authenticated locally with `secrets:write` permission).

### S3-compatible storage

Any S3-compatible store works — Cloudflare R2, MinIO, Backblaze B2, etc. Set `AWS_ENDPOINT_URL_S3` (or `METAFLOW_S3_ENDPOINT_URL`) on both your local machine and as a GitHub repo variable. GHA runners have full internet egress and can reach any public S3-compatible endpoint.

## Development

```bash
git clone https://github.com/npow/metaflow-gha
cd metaflow-gha
pip install -e ".[dev]"
pytest -v
```

CI runs on Python 3.10, 3.11, and 3.12. E2E runs on GitHub Actions against a real S3 datastore.

## License

[Apache 2.0](LICENSE)
