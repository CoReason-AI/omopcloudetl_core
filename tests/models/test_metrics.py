# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import pytest
from pydantic import ValidationError
from omopcloudetl_core.models.metrics import LoadMetrics, ExecutionMetrics


def test_load_metrics_creation():
    """Tests successful creation of LoadMetrics."""
    metrics = LoadMetrics(
        rows_inserted=100,
        rows_rejected=5,
        rows_processed=105,
        error_details_uri="s3://errors/details.csv",
        query_id="qid-123",
    )
    assert metrics.rows_inserted == 100
    assert metrics.rows_rejected == 5
    assert metrics.rows_processed == 105
    assert metrics.error_details_uri == "s3://errors/details.csv"
    assert metrics.query_id == "qid-123"


def test_load_metrics_missing_required_fields():
    """Tests that LoadMetrics raises ValidationError for missing required fields."""
    with pytest.raises(ValidationError):
        LoadMetrics(rows_processed=100)  # rows_inserted and rows_rejected are required


def test_load_metrics_optional_fields():
    """Tests that LoadMetrics can be created with only required fields."""
    metrics = LoadMetrics(rows_inserted=50, rows_rejected=0)
    assert metrics.rows_inserted == 50
    assert metrics.rows_rejected == 0
    assert metrics.rows_processed is None
    assert metrics.error_details_uri is None
    assert metrics.query_id is None


def test_execution_metrics_creation():
    """Tests successful creation of ExecutionMetrics."""
    metrics = ExecutionMetrics(rows_affected=10, rows_inserted=5, rows_updated=3, rows_deleted=2, query_id="qid-456")
    assert metrics.rows_affected == 10
    assert metrics.rows_inserted == 5
    assert metrics.rows_updated == 3
    assert metrics.rows_deleted == 2
    assert metrics.query_id == "qid-456"


def test_execution_metrics_all_fields_optional():
    """Tests that ExecutionMetrics can be created with no fields."""
    metrics = ExecutionMetrics()
    assert metrics.rows_affected is None
    assert metrics.rows_inserted is None
    assert metrics.rows_updated is None
    assert metrics.rows_deleted is None
    assert metrics.query_id is None


def test_execution_metrics_partial_fields():
    """Tests creation of ExecutionMetrics with a subset of fields."""
    metrics = ExecutionMetrics(rows_inserted=5, query_id="qid-789")
    assert metrics.rows_inserted == 5
    assert metrics.query_id == "qid-789"
    assert metrics.rows_affected is None
