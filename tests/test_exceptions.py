import pytest
from omopcloudetl_core import exceptions


def test_exception_hierarchy():
    """Test that all custom exceptions inherit from the base exception."""
    assert issubclass(exceptions.ConfigurationError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.DatabaseConnectionError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.WorkflowError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.DiscoveryError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.SpecificationError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.SecretAccessError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.DMLValidationError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.CompilationError, exceptions.OmopCloudEtlError)
    assert issubclass(exceptions.SQLExecutionError, exceptions.OmopCloudEtlError)


def test_sql_execution_error_attributes():
    """Test that SQLExecutionError correctly stores all its custom attributes."""
    failed_sql = "SELECT * FROM non_existent_table;"
    underlying_error = "Table not found"
    step_name = "test_step"
    query_id = "qid-12345"
    message = "SQL execution failed"

    exc = exceptions.SQLExecutionError(
        message=message,
        failed_sql=failed_sql,
        underlying_error=underlying_error,
        step_name=step_name,
        query_id=query_id,
    )

    assert exc.failed_sql == failed_sql
    assert exc.underlying_error == underlying_error
    assert exc.step_name == step_name
    assert exc.query_id == query_id
    assert str(exc).startswith(message)


def test_sql_execution_error_str_representation():
    """Test the __str__ method of SQLExecutionError for correct formatting."""
    exc = exceptions.SQLExecutionError(
        message="Execution failed",
        failed_sql="SELECT 1",
        underlying_error="DB Error",
        step_name="step_one",
        query_id="query-abc",
    )

    error_str = str(exc)

    assert "Execution failed" in error_str
    assert "[Step: step_one, QueryID: query-abc]" in error_str
    assert "Underlying Error: DB Error" in error_str
    assert "Failed SQL: SELECT 1" in error_str


def test_raising_exceptions():
    """Test that the custom exceptions can be raised and caught correctly."""
    with pytest.raises(exceptions.ConfigurationError):
        raise exceptions.ConfigurationError("A config error occurred")

    with pytest.raises(exceptions.OmopCloudEtlError):
        raise exceptions.DMLValidationError("A DML validation error occurred")
