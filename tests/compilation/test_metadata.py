# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

from omopcloudetl_core.compilation.metadata import MetadataManager
from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics


@pytest.fixture
def mock_connection(mocker: MockerFixture):
    """Fixture to create a mock database connection."""
    return mocker.Mock()


@pytest.fixture
def manager(mock_connection):
    """Fixture to create a MetadataManager instance."""
    return MetadataManager(mock_connection, "test_schema")


def test_initialize_store(manager: MetadataManager, mock_connection):
    """Test that the DDL for creating the log table is executed."""
    manager.initialize_store()
    mock_connection.execute_sql.assert_called_once()
    sql_call = mock_connection.execute_sql.call_args[0][0]
    assert f"CREATE TABLE IF NOT EXISTS {manager.qualified_table_name}" in sql_call


def test_log_step_start(manager: MetadataManager, mock_connection):
    """Test the INSERT statement and parameters for logging a step start."""
    execution_id = uuid4()
    log_id = 123
    manager.log_step_start(execution_id, "test_step", log_id)

    mock_connection.execute_sql.assert_called_once()
    args, kwargs = mock_connection.execute_sql.call_args
    sql_query = args[0]
    params = kwargs["params"]

    assert f"INSERT INTO {manager.qualified_table_name}" in sql_query
    assert "status" in sql_query
    assert "start_time" in sql_query

    assert log_id == params[0]
    assert str(execution_id) == params[1]
    assert "test_step" == params[2]


def test_log_step_end_success_with_load_metrics(manager: MetadataManager, mock_connection):
    """Test the UPDATE statement for a successful step with LoadMetrics."""
    log_id = 456
    metrics = LoadMetrics(rows_processed=1000, rows_inserted=990, rows_rejected=10, query_id="q1")
    manager.log_step_end(log_id, "SUCCESS", metrics)

    mock_connection.execute_sql.assert_called_once()
    args, kwargs = mock_connection.execute_sql.call_args
    sql_query = args[0]
    params = kwargs["params"]

    assert f"UPDATE {manager.qualified_table_name}" in sql_query
    assert "duration_seconds" in sql_query

    assert "SUCCESS" == params[0]
    assert metrics.rows_processed == params[4]
    assert metrics.rows_inserted == params[5]
    assert metrics.rows_rejected == params[6]
    assert log_id == params[-1]


def test_log_step_end_failure_with_error_message(manager: MetadataManager, mock_connection):
    """Test the UPDATE statement for a failed step with an error message."""
    log_id = 789
    error_msg = "Something went wrong"
    manager.log_step_end(log_id, "FAILURE", error_message=error_msg)

    mock_connection.execute_sql.assert_called_once()
    args, kwargs = mock_connection.execute_sql.call_args
    params = kwargs["params"]

    assert "FAILURE" == params[0]
    assert error_msg == params[9]
    assert log_id == params[-1]


def test_log_step_end_with_execution_metrics(manager: MetadataManager, mock_connection):
    """Test the UPDATE statement for a step with ExecutionMetrics."""
    log_id = 101
    metrics = ExecutionMetrics(rows_affected=150, rows_inserted=50, query_id="q2")
    manager.log_step_end(log_id, "SUCCESS", metrics)

    mock_connection.execute_sql.assert_called_once()
    args, kwargs = mock_connection.execute_sql.call_args
    params = kwargs["params"]

    assert "SUCCESS" == params[0]
    assert metrics.rows_affected == params[3]
    assert metrics.rows_inserted == params[5]
    assert metrics.query_id == params[8]
    assert log_id == params[-1]
