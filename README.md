> ⚠️ **Work in progress.** Not ready for production use.

# metaflow-gha

[![CI](https://github.com/npow/metaflow-gha/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-gha/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-gha)](https://pypi.org/project/metaflow-gha/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

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

## Install

```bash
pip install metaflow-gha
```

Requires:
- Python 3.8+
- `metaflow`
- `boto3` (S3 datastore — `--datastore=s3` required)
- [`gh` CLI](https://cli.github.com/) authenticated with `gh auth login`

## Usage

**Apply to the whole flow via CLI:**

```bash
python flow.py run --with=gha
```

**Apply to specific steps with the decorator:**

```python
from metaflow import FlowSpec, step
from metaflow_extensions.gha.plugins.gha_decorator import gha

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

**Control parallelism and timeout:**

```python
@gha(workers=10, timeout=3600, max_retries=1)
@step
def train(self):
    ...
```

## How it works

When `@gha` is active, each step task is pushed to an S3-backed queue instead of running locally. Up to 20 GitHub Actions jobs (one per runner VM) are dispatched once per run via the `gh` CLI. Each worker pulls tasks from the queue, downloads the code package, and runs `python flow.py step ...` — standard Metaflow step execution. Workers use S3 conditional writes (`IfNoneMatch: *`) for atomic task claiming, so there are no races even with 20 concurrent workers.

The queue lives entirely in your Metaflow S3 datastore — no extra infrastructure required.

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `METAFLOW_DATASTORE_SYSROOT_S3` | — | Required. Your Metaflow S3 root (e.g. `s3://my-bucket/metaflow`). |
| `METAFLOW_GHA_USER_REPO` | inferred from `git remote` | GitHub repo to dispatch workers on (e.g. `myorg/myrepo`). |
| `METAFLOW_GHA_WORKER_REPO` | `npow/metaflow-gha` | Repo hosting the reusable worker workflow. |
| `METAFLOW_GHA_CALLER_WORKFLOW` | `metaflow-gha.yml` | Workflow filename in your repo (written by `gha inject`). |

AWS credentials for the worker VMs are passed via repository secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`.

## Development

```bash
git clone https://github.com/npow/metaflow-gha
cd metaflow-gha
pip install -e ".[dev]"
pytest -v
```

## License

[Apache 2.0](LICENSE)
