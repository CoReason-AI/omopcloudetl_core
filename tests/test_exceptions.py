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
from omopcloudetl_core.exceptions import (
    OmopCloudEtlError,
    SQLExecutionError,
    ConfigurationError,
)


def test_base_exception_can_be_raised():
    """Tests that the base exception can be raised."""
    with pytest.raises(OmopCloudEtlError):
        raise OmopCloudEtlError("A base error occurred.")


def test_sql_execution_error_attributes():
    """Tests that SQLExecutionError correctly stores its custom attributes."""
    failed_sql = "SELECT * FROM non_existent_table;"
    underlying_error = "Table not found"
    step_name = "test_step"
    query_id = "query123"

    exc = SQLExecutionError(
        message="SQL execution failed",
        failed_sql=failed_sql,
        underlying_error=underlying_error,
        step_name=step_name,
        query_id=query_id,
    )

    assert exc.failed_sql == failed_sql
    assert exc.underlying_error == underlying_error
    assert exc.step_name == step_name
    assert exc.query_id == query_id
    assert step_name in str(exc)
    assert query_id in str(exc)
    assert failed_sql in str(exc)


def test_all_exceptions_can_be_raised():
    """Tests that all defined custom exceptions can be raised."""
    exceptions_to_test = [
        exc
        for exc in OmopCloudEtlError.__subclasses__()
        if exc is not SQLExecutionError  # Tested separately due to different constructor
    ]
    assert len(exceptions_to_test) > 5  # Sanity check that we found subclasses

    for exc_class in exceptions_to_test:
        with pytest.raises(exc_class):
            raise exc_class(f"Testing {exc_class.__name__}")
