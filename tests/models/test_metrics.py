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


def test_load_metrics_successful_validation():
    """Tests that LoadMetrics validates with correct data."""
    data = {"rows_inserted": 100, "rows_rejected": 5, "query_id": "qid_123"}
    metrics = LoadMetrics.model_validate(data)
    assert metrics.rows_inserted == 100
    assert metrics.rows_rejected == 5
    assert metrics.query_id == "qid_123"


def test_load_metrics_missing_required_fields():
    """Tests that LoadMetrics fails validation if required fields are missing."""
    with pytest.raises(ValidationError):
        LoadMetrics.model_validate({"rows_rejected": 5})  # missing rows_inserted


def test_execution_metrics_all_fields():
    """Tests that ExecutionMetrics validates with a full set of data."""
    data = {"rows_affected": 25, "rows_inserted": 10, "rows_updated": 5, "rows_deleted": 10, "query_id": "qid_456"}
    metrics = ExecutionMetrics.model_validate(data)
    assert metrics.rows_affected == 25
    assert metrics.rows_inserted == 10
    assert metrics.rows_updated == 5
    assert metrics.rows_deleted == 10


def test_execution_metrics_optional_fields():
    """Tests that ExecutionMetrics can be created with only optional fields."""
    metrics = ExecutionMetrics()
    assert metrics.rows_affected is None
    assert metrics.rows_inserted is None
