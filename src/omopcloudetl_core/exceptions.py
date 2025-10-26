class OmopCloudEtlError(Exception):
    """Base exception for the omopcloudetl_core package."""
    pass


class ConfigurationError(OmopCloudEtlError):
    """Raised for configuration-related errors."""
    pass


class DatabaseConnectionError(OmopCloudEtlError):
    """Raised for database connection errors."""
    pass


class WorkflowError(OmopCloudEtlError):
    """Raised for workflow-related errors."""
    pass


class DiscoveryError(OmopCloudEtlError):
    """Raised for discovery-related errors."""
    pass


class SpecificationError(OmopCloudEtlError):
    """Raised for specification-related errors."""
    pass


class SecretAccessError(OmopCloudEtlError):
    """Raised for secret access errors."""
    pass


class DMLValidationError(OmopCloudEtlError):
    """Raised for DML validation errors."""
    pass


class CompilationError(OmopCloudEtlError):
    """Raised for failures during DML to SQL translation."""
    pass


class SQLExecutionError(OmopCloudEtlError):
    """Raised for SQL execution errors."""
    def __init__(self, message, failed_sql, underlying_error, step_name, query_id):
        super().__init__(message)
        self.failed_sql = failed_sql
        self.underlying_error = underlying_error
        self.step_name = step_name
        self.query_id = query_id
