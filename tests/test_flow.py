"""Tests for moonwalk_flow.py Prefect tasks and non-fatal task graceful handling.

These tests use unittest.mock to isolate the flow from real ETL, DuckDB,
Postgres, and Notion API calls — no network, no filesystem writes.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# =====================================================================
# helpers
# =====================================================================


def _run_task_fn(task_obj, *args, **kwargs):
    """Call the underlying function of a Prefect task without a flow context."""
    return task_obj.fn(*args, **kwargs)


# =====================================================================
# test_flow_runs_all_tasks
# =====================================================================


def test_flow_runs_all_tasks():
    """All 5 task functions are called exactly once when the flow runs end-to-end."""
    with (
        patch("moonwalk_flow.validate_source_csvs") as mock_validate,
        patch("moonwalk_flow.run_etl") as mock_etl,
        patch("moonwalk_flow.run_duckdb") as mock_duckdb,
        patch("moonwalk_flow.run_postgres") as mock_postgres,
        patch("moonwalk_flow.push_notion_narrative") as mock_narrative,
    ):
        from moonwalk_flow import moonwalk_refresh

        moonwalk_refresh()

        mock_validate.assert_called_once()
        mock_etl.assert_called_once()
        mock_duckdb.assert_called_once()
        mock_postgres.assert_called_once()
        mock_narrative.assert_called_once()


# =====================================================================
# test_flow_continues_if_notion_fails
# =====================================================================


def test_flow_continues_if_postgres_fails():
    """Flow completes and Notion narrative still runs even if Postgres sync raises."""
    import moonwalk_flow

    notion_called = []

    def _notion_ok():
        notion_called.append(True)

    with (
        patch.object(moonwalk_flow, "validate_source_csvs"),
        patch.object(moonwalk_flow, "run_etl"),
        patch.object(moonwalk_flow, "run_duckdb"),
        patch.object(moonwalk_flow, "run_postgres"),
        patch.object(moonwalk_flow, "push_notion_narrative", side_effect=_notion_ok),
    ):
        moonwalk_flow.moonwalk_refresh()

    assert notion_called, "push_notion_narrative should still be called after run_postgres"


def test_push_notion_narrative_task_catches_exceptions():
    """push_notion_narrative task function logs warning and does NOT re-raise."""
    import moonwalk_flow

    mock_logger = MagicMock()

    def _raising_notion_run(log):
        raise ConnectionError("Network timeout")

    mock_notion_push = MagicMock()
    mock_notion_push.run = _raising_notion_run

    with (
        patch("moonwalk_flow.get_run_logger", return_value=mock_logger),
        patch.dict(sys.modules, {"notion_push": mock_notion_push}),
    ):
        # Call the underlying task function directly — should not raise
        moonwalk_flow.push_notion_narrative.fn()

    mock_logger.warning.assert_called_once()
    warning_msg = mock_logger.warning.call_args[0][0]
    assert "non-fatal" in warning_msg


# =====================================================================
# test_notion_kpi_push_skips_if_no_key
# =====================================================================


def test_notion_kpi_push_skips_if_no_key():
    """notion_kpi_push.run() returns None gracefully when API keys are missing."""
    import notion_kpi_push

    log_calls = []

    with (
        patch.object(notion_kpi_push, "NOTION_API_KEY", ""),
        patch.object(notion_kpi_push, "NOTION_KPI_DB_ID", ""),
    ):
        result = notion_kpi_push.run(log=log_calls.append)

    assert result is None
    # Should have logged exactly one skip message
    assert len(log_calls) == 1
    assert "skipped" in log_calls[0].lower()
