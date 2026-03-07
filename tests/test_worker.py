"""Unit tests for worker._build_step_command."""

import sys

# Stub out metaflow_coordinator before importing worker
import sys as _sys
import types
from unittest.mock import MagicMock

_mf_coord = types.ModuleType("metaflow_coordinator")
_mf_coord_s3 = types.ModuleType("metaflow_coordinator.s3_queue")
for _name in [
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
    "_claimed_key",
    "_ready_key",
    "_waiting_key",
]:
    setattr(_mf_coord_s3, _name, MagicMock())
_mf_coord.s3_queue = _mf_coord_s3
_sys.modules.setdefault("metaflow_coordinator", _mf_coord)
_sys.modules.setdefault("metaflow_coordinator.s3_queue", _mf_coord_s3)

from metaflow_extensions.gha.plugins.worker import _build_step_command  # noqa: E402

BASE_TASK = {
    "flow_name": "MyFlow",
    "flow_file": "my_flow.py",
    "step_name": "train",
    "run_id": "123",
    "task_id": "456",
    "attempt": 0,
    "max_user_code_retries": 0,
}


def test_basic_command():
    cmd = _build_step_command(BASE_TASK, "/workdir")
    assert cmd[0] == sys.executable
    assert "my_flow.py" in cmd
    assert "--datastore=s3" in cmd
    assert "step" in cmd
    assert "train" in cmd
    assert "--run-id=123" in cmd
    assert "--task-id=456" in cmd
    assert "--retry-count=0" in cmd


def test_flow_file_fallback():
    task = {**BASE_TASK, "flow_file": None}
    cmd = _build_step_command(task, "/workdir")
    assert "MyFlow.py" in cmd


def test_input_paths():
    task = {**BASE_TASK, "input_paths": "123/start/456"}
    cmd = _build_step_command(task, "/workdir")
    assert "--input-paths=123/start/456" in cmd


def test_no_input_paths_by_default():
    cmd = _build_step_command(BASE_TASK, "/workdir")
    assert not any(a.startswith("--input-paths") for a in cmd)


def test_split_index():
    task = {**BASE_TASK, "split_index": 3}
    cmd = _build_step_command(task, "/workdir")
    assert "--split-index=3" in cmd


def test_split_index_zero():
    task = {**BASE_TASK, "split_index": 0}
    cmd = _build_step_command(task, "/workdir")
    assert "--split-index=0" in cmd


def test_no_split_index_by_default():
    cmd = _build_step_command(BASE_TASK, "/workdir")
    assert not any(a.startswith("--split-index") for a in cmd)


def test_namespace():
    task = {**BASE_TASK, "namespace": "user:alice"}
    cmd = _build_step_command(task, "/workdir")
    assert "--namespace=user:alice" in cmd


def test_ubf_context():
    task = {**BASE_TASK, "ubf_context": "ubf_task"}
    cmd = _build_step_command(task, "/workdir")
    assert "--ubf-context=ubf_task" in cmd


def test_tags():
    task = {**BASE_TASK, "tag": ["v1", "prod"]}
    cmd = _build_step_command(task, "/workdir")
    assert "--tag=v1" in cmd
    assert "--tag=prod" in cmd


def test_no_tags_by_default():
    cmd = _build_step_command(BASE_TASK, "/workdir")
    assert not any(a.startswith("--tag") for a in cmd)


def test_retry_count():
    task = {**BASE_TASK, "attempt": 2}
    cmd = _build_step_command(task, "/workdir")
    assert "--retry-count=2" in cmd


def test_max_user_code_retries():
    task = {**BASE_TASK, "max_user_code_retries": 5}
    cmd = _build_step_command(task, "/workdir")
    assert "--max-user-code-retries=5" in cmd


# ---------------------------------------------------------------------------
# _setup_environment
# ---------------------------------------------------------------------------


def test_setup_environment_no_requirements(tmp_path):
    from metaflow_extensions.gha.plugins.worker import _setup_environment

    _setup_environment(str(tmp_path), env_id=None)  # no error = pass


def test_setup_environment_with_requirements(tmp_path):
    from unittest.mock import patch

    from metaflow_extensions.gha.plugins.worker import _setup_environment

    req = tmp_path / "requirements.txt"
    req.write_text("requests\n")

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        _setup_environment(str(tmp_path), env_id="abc123")

    assert mock_run.called
    call_args = mock_run.call_args[0][0]
    assert "-r" in call_args
    assert str(req) in call_args


# ---------------------------------------------------------------------------
# worker idle exit
# ---------------------------------------------------------------------------


def test_worker_exits_after_idle_timeout():
    import time
    from unittest.mock import MagicMock, patch

    mock_client = MagicMock()
    mock_s3 = MagicMock()
    mock_client.claim_task.return_value = None  # always empty

    with patch("metaflow_extensions.gha.plugins.worker.make_s3_client", return_value=mock_s3):
        with patch("metaflow_extensions.gha.plugins.worker.S3QueueClient") as mock_cls:
            with patch("time.sleep"):
                mock_cls.from_env.return_value = mock_client
                from metaflow_extensions.gha.plugins.worker import run_worker

                t0 = time.monotonic()
                call_n = 0

                def fake_monotonic():
                    nonlocal call_n
                    call_n += 1
                    return t0 if call_n <= 2 else t0 + 9999

                with patch("time.monotonic", side_effect=fake_monotonic):
                    run_worker(run_id="run1", worker_id="w1", max_idle_seconds=1)
    # Should exit cleanly without raising
