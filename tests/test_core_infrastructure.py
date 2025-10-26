import pytest
import sys
from omopcloudetl_core.exceptions import OmopCloudEtlError, SQLExecutionError
from omopcloudetl_core.logging import logger

def test_raise_custom_exception():
    """Tests that the custom exception can be raised."""
    with pytest.raises(OmopCloudEtlError):
        raise OmopCloudEtlError("This is a test error.")

def test_sql_execution_error_captures_details():
    """Tests that the SQLExecutionError captures all the required details."""
    try:
        raise SQLExecutionError(
            message="SQL execution failed.",
            failed_sql="SELECT * FROM my_table;",
            underlying_error="Syntax error.",
            step_name="test_step",
            query_id="12345"
        )
    except SQLExecutionError as e:
        assert str(e) == "SQL execution failed."
        assert e.failed_sql == "SELECT * FROM my_table;"
        assert e.underlying_error == "Syntax error."
        assert e.step_name == "test_step"
        assert e.query_id == "12345"

def test_logger_initialization(capsys):
    """Tests that the logger can be initialized and logs messages."""
    # Reconfigure the logger for synchronous testing to ensure output is captured
    logger.remove()
    logger.add(sys.stderr, format="{message}", enqueue=False, colorize=False)

    logger.info("This is a test log message.")
    captured = capsys.readouterr()
    assert captured.err.strip() == "This is a test log message."
