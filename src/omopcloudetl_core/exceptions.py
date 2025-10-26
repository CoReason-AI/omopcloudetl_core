"""Custom exception types for the omopcloudetl_core package."""

from typing import Optional


class OmopCloudEtlError(Exception):
    """Base exception for all application-specific errors."""

    pass


class ConfigurationError(OmopCloudEtlError):
    """Raised for configuration-related errors."""

    pass


class DatabaseConnectionError(OmopCloudEtlError):
    """Raised for errors related to database connectivity."""

    pass


class WorkflowError(OmopCloudEtlError):
    """Raised for errors during workflow definition or execution."""

    pass


class DiscoveryError(OmopCloudEtlError):
    """Raised for errors during component discovery."""

    pass


class SpecificationError(OmopCloudEtlError):
    """Raised for errors related to CDM specification handling."""

    pass


class SecretAccessError(OmopCloudEtlError):
    """Raised when a secret cannot be accessed."""

    pass


class DMLValidationError(OmopCloudEtlError):
    """Raised for errors in DML definition files."""

    pass


class CompilationError(OmopCloudEtlError):
    """Raised for failures during the DML to SQL compilation process."""

    pass


class SQLExecutionError(OmopCloudEtlError):
    """
    Raised when a SQL execution fails.

    Attributes:
        failed_sql (str): The SQL statement that failed.
        underlying_error (str): The string representation of the original database error.
        step_name (Optional[str]): The name of the workflow step where the error occurred.
        query_id (Optional[str]): The database-specific query ID, if available.
    """

    def __init__(
        self,
        message: str,
        failed_sql: str,
        underlying_error: str,
        step_name: Optional[str] = None,
        query_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.failed_sql = failed_sql
        self.underlying_error = underlying_error
        self.step_name = step_name
        self.query_id = query_id

    def __str__(self) -> str:
        return (
            f"{super().__str__()} "
            f"[Step: {self.step_name}, QueryID: {self.query_id}] "
            f"Underlying Error: {self.underlying_error} "
            f"Failed SQL: {self.failed_sql[:500]}..."
        )
