# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from unittest.mock import MagicMock
from uuid import uuid4
import pytest
from omopcloudetl_core.compilation.metadata import MetadataManager
from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics
from datetime import datetime


@pytest.fixture
def mock_connection():
    """Fixture to create a mock BaseConnection."""
    return MagicMock()


@pytest.fixture
def metadata_manager(mock_connection):
    """Fixture to create a MetadataManager with a mock connection."""
    return MetadataManager(mock_connection)


def test_initialize_store(metadata_manager, mock_connection):
    """Test that the metadata table is created correctly."""
    execution_id = uuid4()
    metadata_manager.initialize_store(execution_id)

    assert mock_connection.execute_sql.call_count == 1
    args, kwargs = mock_connection.execute_sql.call_args
    sql_call = args[0]

    assert f"CREATE TABLE IF NOT EXISTS {MetadataManager.METADATA_TABLE}" in sql_call
    assert "log_id BIGINT" in sql_call
    assert "execution_id VARCHAR(36)" in sql_call


def test_log_step_start(metadata_manager, mock_connection):
    """Test logging the start of a workflow step."""
    execution_id = uuid4()
    step_name = "test_step"
    metadata_manager.log_step_start(execution_id, step_name)

    assert mock_connection.execute_sql.call_count == 1
    args, kwargs = mock_connection.execute_sql.call_args
    sql_call = args[0]
    params = kwargs["params"]

    assert f"INSERT INTO {MetadataManager.METADATA_TABLE}" in sql_call
    assert "(?, ?, ?, ?)" in sql_call
    assert params[0] == str(execution_id)
    assert params[1] == step_name
    assert params[2] == "RUNNING"
    assert isinstance(params[3], datetime)


def test_log_step_end_success_execution_metrics(metadata_manager, mock_connection):
    """Test logging the end of a successful step with ExecutionMetrics."""
    execution_id = uuid4()
    step_name = "transform_step"
    metrics = ExecutionMetrics(
        rows_affected=100,
        rows_inserted=80,
        query_id="query-123",
    )

    metadata_manager.log_step_end(execution_id, step_name, metrics)

    assert mock_connection.execute_sql.call_count == 1
    args, kwargs = mock_connection.execute_sql.call_args
    sql_call = args[0]
    params = kwargs["params"]

    assert f"UPDATE {MetadataManager.METADATA_TABLE}" in sql_call
    assert "status = ?" in sql_call
    assert "rows_affected = ?" in sql_call
    assert "rows_inserted = ?" in sql_call
    assert "query_id = ?" in sql_call
    assert "duration_seconds = EXTRACT(EPOCH FROM (end_time - start_time))" in sql_call
    assert params[0] == "COMPLETED"
    assert isinstance(params[1], datetime)  # end_time
    assert params[2] == 100  # rows_affected
    assert params[3] == 80  # rows_inserted
    assert params[4] == "query-123"


def test_log_step_end_success_load_metrics(metadata_manager, mock_connection):
    """Test logging the end of a successful step with LoadMetrics."""
    execution_id = uuid4()
    step_name = "load_step"
    metrics = LoadMetrics(
        rows_processed=1000,
        rows_inserted=990,
        rows_rejected=10,
        error_details_uri="s3://errors/details.csv",
        query_id="query-456",
    )

    metadata_manager.log_step_end(execution_id, step_name, metrics)

    assert mock_connection.execute_sql.call_count == 1
    args, kwargs = mock_connection.execute_sql.call_args
    sql_call = args[0]
    params = kwargs["params"]

    assert f"UPDATE {MetadataManager.METADATA_TABLE}" in sql_call
    assert "status = ?" in sql_call
    assert "rows_processed = ?" in sql_call
    assert "rows_inserted = ?" in sql_call
    assert "query_id = ?" in sql_call
    assert "duration_seconds = EXTRACT(EPOCH FROM (end_time - start_time))" in sql_call
    assert params[0] == "COMPLETED"
    assert isinstance(params[1], datetime)  # end_time
    assert params[2] == 1000  # rows_processed
    assert params[3] == 990  # rows_inserted
    assert params[4] == "query-456"


def test_log_step_end_failure(metadata_manager, mock_connection):
    """Test logging the end of a failed step."""
    execution_id = uuid4()
    step_name = "failed_step"
    error = ValueError("Something went wrong")

    metadata_manager.log_step_end(execution_id, step_name, metrics=None, error=error)

    assert mock_connection.execute_sql.call_count == 1
    args, kwargs = mock_connection.execute_sql.call_args
    sql_call = args[0]
    params = kwargs["params"]

    assert f"UPDATE {MetadataManager.METADATA_TABLE}" in sql_call
    assert "status = ?" in sql_call
    assert "error_message = ?" in sql_call
    assert "duration_seconds = EXTRACT(EPOCH FROM (end_time - start_time))" in sql_call
    assert params[0] == "FAILED"
    assert isinstance(params[1], datetime)  # end_time
    assert params[2] == "Something went wrong"
