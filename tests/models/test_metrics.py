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


def test_load_metrics_valid():
    """Test successful creation of a LoadMetrics instance with all fields."""
    metrics = LoadMetrics(
        rows_processed=1000,
        rows_inserted=990,
        rows_rejected=10,
        error_details_uri="s3://my-bucket/errors/load1.csv",
        query_id="qid-load-123",
    )
    assert metrics.rows_processed == 1000
    assert metrics.rows_inserted == 990
    assert metrics.rows_rejected == 10
    assert metrics.error_details_uri == "s3://my-bucket/errors/load1.csv"
    assert metrics.query_id == "qid-load-123"


def test_load_metrics_missing_required():
    """Test that LoadMetrics fails validation if required fields are missing."""
    with pytest.raises(ValidationError):
        LoadMetrics(rows_processed=100)  # Missing rows_inserted and rows_rejected


def test_load_metrics_optional_fields():
    """Test creating a LoadMetrics instance with only the required fields."""
    metrics = LoadMetrics(rows_inserted=500, rows_rejected=0)
    assert metrics.rows_inserted == 500
    assert metrics.rows_rejected == 0
    assert metrics.rows_processed is None
    assert metrics.error_details_uri is None
    assert metrics.query_id is None


def test_execution_metrics_all_fields():
    """Test successful creation of an ExecutionMetrics instance."""
    metrics = ExecutionMetrics(
        rows_affected=150,
        rows_inserted=100,
        rows_updated=50,
        rows_deleted=0,
        query_id="qid-exec-456",
    )
    assert metrics.rows_affected == 150
    assert metrics.rows_inserted == 100
    assert metrics.rows_updated == 50
    assert metrics.rows_deleted == 0
    assert metrics.query_id == "qid-exec-456"


def test_execution_metrics_no_required_fields():
    """Test that ExecutionMetrics can be created with no fields, as all are optional."""
    metrics = ExecutionMetrics()
    assert metrics.rows_affected is None
    assert metrics.rows_inserted is None
    assert metrics.rows_updated is None
    assert metrics.rows_deleted is None
    assert metrics.query_id is None
