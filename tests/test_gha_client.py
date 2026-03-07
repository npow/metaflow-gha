"""Unit tests for GHAClient."""

import os
import tempfile

import pytest

from metaflow_extensions.gha.plugins.gha_client import GHAClient, GHAClientError


def make_client(**kwargs):
    defaults = dict(
        user_repo="myorg/myrepo",
        worker_repo="npow/metaflow-gha",
        worker_workflow="worker.yml",
        caller_workflow="metaflow-gha.yml",
        worker_ref="main",
        dispatch_ref="",
    )
    return GHAClient(**{**defaults, **kwargs})


# ---------------------------------------------------------------------------
# inject_caller_workflow
# ---------------------------------------------------------------------------


def test_inject_creates_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        os.chdir(tmpdir)
        try:
            client = make_client()
            path = client.inject_caller_workflow()
            assert os.path.exists(path)
            assert path == ".github/workflows/metaflow-gha.yml"
        finally:
            os.chdir(original_dir)


def test_inject_workflow_contains_worker_repo():
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        os.chdir(tmpdir)
        try:
            client = make_client(worker_repo="myorg/metaflow-gha")
            path = client.inject_caller_workflow()
            content = open(path).read()
            assert "myorg/metaflow-gha" in content
        finally:
            os.chdir(original_dir)


def test_inject_workflow_contains_worker_ref():
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        os.chdir(tmpdir)
        try:
            client = make_client(worker_ref="v1.2.3")
            path = client.inject_caller_workflow()
            content = open(path).read()
            assert "v1.2.3" in content
        finally:
            os.chdir(original_dir)


def test_inject_workflow_contains_worker_workflow():
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        os.chdir(tmpdir)
        try:
            client = make_client(worker_workflow="custom-worker.yml")
            path = client.inject_caller_workflow()
            content = open(path).read()
            assert "custom-worker.yml" in content
        finally:
            os.chdir(original_dir)


def test_inject_workflow_idempotent():
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        os.chdir(tmpdir)
        try:
            client = make_client()
            client.inject_caller_workflow()
            path = client.inject_caller_workflow()  # second call should not raise
            assert os.path.exists(path)
        finally:
            os.chdir(original_dir)


# ---------------------------------------------------------------------------
# from_env
# ---------------------------------------------------------------------------


def test_from_env_uses_env_var(monkeypatch):
    import metaflow_extensions.gha.plugins.gha_client as _mod

    monkeypatch.setattr(_mod, "_GHA_USER_REPO", "envorg/envrepo")
    monkeypatch.setattr(_mod, "_GHA_WORKER_REPO", "npow/metaflow-gha")
    monkeypatch.setattr(_mod, "_GHA_WORKER_WORKFLOW", "worker.yml")
    monkeypatch.setattr(_mod, "_GHA_WORKER_REF", "main")
    monkeypatch.setattr(_mod, "_GHA_CALLER_WORKFLOW", "metaflow-gha.yml")
    monkeypatch.setattr(_mod, "_GHA_DISPATCH_REF", "")
    client = GHAClient.from_env()
    assert client.user_repo == "envorg/envrepo"


def test_from_env_raises_without_repo(monkeypatch, tmp_path):
    monkeypatch.delenv("METAFLOW_GHA_USER_REPO", raising=False)
    # Run from a dir with no git remote
    original_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        with pytest.raises(GHAClientError, match="Cannot determine GitHub repo"):
            GHAClient.from_env()
    finally:
        os.chdir(original_dir)
