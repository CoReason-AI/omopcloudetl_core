from typing import Optional

class OmopCloudEtlError(Exception):
    """Base exception for all omopcloudetl-core errors."""
    pass

class ConfigurationError(OmopCloudEtlError):
    """Raised when there is an error in the project configuration."""
    pass

class DatabaseConnectionError(OmopCloudEtlError):
    """Raised when there is an error connecting to the database."""
    pass

class WorkflowError(OmopCloudEtlError):
    """Raised when there is an error in the workflow definition or execution."""
    pass

class DiscoveryError(OmopCloudEtlError):
    """Raised when there is an error discovering a provider or orchestrator."""
    pass

class SpecificationError(OmopCloudEtlError):
    """Raised when there is an error fetching or parsing a CDM specification."""
    pass

class SecretAccessError(OmopCloudEtlError):
    """Raised when a secret cannot be accessed."""
    pass

class DMLValidationError(OmopCloudEtlError):
    """Raised when a DML definition fails validation."""
    pass

class CompilationError(OmopCloudEtlError):
    """Raised when there is an error compiling a DML definition to SQL."""
    pass

class SQLExecutionError(OmopCloudEtlError):
    """Raised when there is an error executing a SQL statement."""
    def __init__(
        self,
        message: str,
        failed_sql: str,
        step_name: str,
        query_id: Optional[str] = None,
        underlying_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.failed_sql = failed_sql
        self.step_name = step_name
        self.query_id = query_id
        self.underlying_error = underlying_error

    def __str__(self):
        return (
            f"{super().__str__()} "
            f"[Step: {self.step_name}, QueryID: {self.query_id}]\\n"
            f"Underlying Error: {self.underlying_error}\\n"
            f"Failed SQL: {self.failed_sql}"
        )
