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


class TestLoadMetrics:
    def test_load_metrics_instantiation(self):
        """Tests successful instantiation of LoadMetrics with all fields."""
        metrics = LoadMetrics(
            rows_processed=1000,
            rows_inserted=990,
            rows_rejected=10,
            error_details_uri="s3://errors/details.csv",
            query_id="qid-12345",
        )
        assert metrics.rows_processed == 1000
        assert metrics.rows_inserted == 990
        assert metrics.rows_rejected == 10
        assert metrics.error_details_uri == "s3://errors/details.csv"
        assert metrics.query_id == "qid-12345"

    def test_load_metrics_optional_fields(self):
        """Tests instantiation with only required fields."""
        metrics = LoadMetrics(rows_inserted=990, rows_rejected=10)
        assert metrics.rows_inserted == 990
        assert metrics.rows_rejected == 10
        assert metrics.rows_processed is None
        assert metrics.error_details_uri is None
        assert metrics.query_id is None

    def test_load_metrics_missing_required_fields(self):
        """Tests that a ValidationError is raised for missing required fields."""
        with pytest.raises(ValidationError):
            LoadMetrics(rows_inserted=990)  # Missing rows_rejected
        with pytest.raises(ValidationError):
            LoadMetrics(rows_rejected=10)  # Missing rows_inserted


class TestExecutionMetrics:
    def test_execution_metrics_instantiation(self):
        """Tests successful instantiation of ExecutionMetrics with all fields."""
        metrics = ExecutionMetrics(
            rows_affected=150,
            rows_inserted=100,
            rows_updated=50,
            rows_deleted=0,
            query_id="qid-abcde",
        )
        assert metrics.rows_affected == 150
        assert metrics.rows_inserted == 100
        assert metrics.rows_updated == 50
        assert metrics.rows_deleted == 0
        assert metrics.query_id == "qid-abcde"

    def test_execution_metrics_all_optional(self):
        """Tests that ExecutionMetrics can be instantiated with no fields."""
        metrics = ExecutionMetrics()
        assert metrics.rows_affected is None
        assert metrics.rows_inserted is None
        assert metrics.rows_updated is None
        assert metrics.rows_deleted is None
        assert metrics.query_id is None

    def test_execution_metrics_partial_fields(self):
        """Tests instantiation with a subset of optional fields."""
        metrics = ExecutionMetrics(rows_inserted=50, query_id="qid-xyz")
        assert metrics.rows_inserted == 50
        assert metrics.query_id == "qid-xyz"
        assert metrics.rows_affected is None
        assert metrics.rows_updated is None
        assert metrics.rows_deleted is None
