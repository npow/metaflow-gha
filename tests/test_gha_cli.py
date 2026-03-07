"""Unit tests for gha_cli helper functions."""
import types
import sys
from unittest.mock import MagicMock, patch, call

import pytest

# Stub metaflow_coordinator before any import
_mf_coord = types.ModuleType("metaflow_coordinator")
_mf_coord_s3 = types.ModuleType("metaflow_coordinator.s3_queue")
for _name in ["push_task", "claim_task", "complete_task", "fail_task",
              "reclaim_stale", "list_pending", "mark_workers_dispatched",
              "write_task_log", "read_task_log", "_bucket_prefix_from_env",
              "_done_key", "_failed_key", "_claimed_key", "_ready_key", "_waiting_key"]:
    setattr(_mf_coord_s3, _name, MagicMock())
_mf_coord.s3_queue = _mf_coord_s3
sys.modules.setdefault("metaflow_coordinator", _mf_coord)
sys.modules.setdefault("metaflow_coordinator.s3_queue", _mf_coord_s3)

from metaflow_extensions.gha.plugins.gha_cli import _extract_parent_task_ids, _wait_for_task


# ---------------------------------------------------------------------------
# _extract_parent_task_ids
# ---------------------------------------------------------------------------

def test_extract_none():
    assert _extract_parent_task_ids(None) == []


def test_extract_empty():
    assert _extract_parent_task_ids("") == []


def test_extract_single():
    result = _extract_parent_task_ids("123/start/456")
    assert result == ["456"]


def test_extract_multiple():
    result = _extract_parent_task_ids("123/start/1,123/start/2,123/start/3")
    assert result == ["1", "2", "3"]


def test_extract_skips_parameters():
    # _parameters/* is synthetic and should be excluded
    result = _extract_parent_task_ids("123/_parameters/0")
    assert result == []


def test_extract_skips_short_parts():
    # Paths with fewer than 3 parts are ignored
    result = _extract_parent_task_ids("123/start")
    assert result == []


def test_extract_mixed_parameters_and_real():
    result = _extract_parent_task_ids("123/_parameters/0,123/start/456")
    assert result == ["456"]


# ---------------------------------------------------------------------------
# _wait_for_task
# ---------------------------------------------------------------------------

def _make_client(states, log_content=None):
    """Return a mock client where get_task_state cycles through `states`."""
    client = MagicMock()
    client.get_task_state.side_effect = states
    client.read_task_log.return_value = log_content
    client.reclaim_stale.return_value = 0
    return client


def test_wait_returns_on_done():
    client = _make_client(["ready", "done"])
    with patch("time.sleep"):
        _wait_for_task(client, "run1", "task1", timeout=60)
    assert client.get_task_state.call_count == 2


def test_wait_raises_on_failed():
    from metaflow._vendor.click import ClickException
    client = _make_client(["ready", "failed"])
    with patch("time.sleep"):
        with pytest.raises(ClickException, match="failed permanently"):
            _wait_for_task(client, "run1", "task1", timeout=60)


def test_wait_times_out():
    from metaflow._vendor.click import ClickException
    import time as _time

    # Make monotonic always exceed deadline immediately after first call
    start = _time.monotonic()
    call_count = 0

    def fake_monotonic():
        nonlocal call_count
        call_count += 1
        # First few calls are setup; then exceed timeout
        return start if call_count <= 3 else start + 9999

    client = _make_client(["ready"] * 100)
    with patch("time.sleep"), patch("metaflow_extensions.gha.plugins.gha_cli.time") as mock_time:
        mock_time.monotonic.side_effect = fake_monotonic
        mock_time.sleep = MagicMock()
        with pytest.raises(ClickException, match="timed out"):
            _wait_for_task(client, "run1", "task1", timeout=30)


def test_wait_streams_logs():
    client = _make_client(["ready", "done"])
    client.read_task_log.return_value = "line1\nline2\nline3"

    output_lines = []
    with patch("time.sleep"), patch("metaflow._vendor.click.echo", side_effect=lambda x: output_lines.append(x)):
        _wait_for_task(client, "run1", "task1", timeout=60)

    assert "line1" in output_lines
    assert "line2" in output_lines
    assert "line3" in output_lines
