# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Optional


class OmopCloudEtlError(Exception):
    """Base exception for all omopcloudetl-core errors."""

    pass


class ConfigurationError(OmopCloudEtlError):
    """Errors related to project or component configuration."""

    pass


class DatabaseConnectionError(OmopCloudEtlError):
    """Errors related to connecting to a database."""

    pass


class WorkflowError(OmopCloudEtlError):
    """Errors related to workflow definition, validation, or execution."""

    pass


class DiscoveryError(OmopCloudEtlError):
    """Errors related to discovering pluggable components."""

    pass


class SpecificationError(OmopCloudEtlError):
    """Errors related to fetching or parsing CDM specifications."""

    pass


class SecretAccessError(OmopCloudEtlError):
    """Errors related to accessing secrets."""

    pass


class DMLValidationError(OmopCloudEtlError):
    """Errors related to the validation of DML definitions."""

    pass


class CompilationError(OmopCloudEtlError):
    """Errors that occur during the compilation of DML to SQL."""

    pass


class SQLExecutionError(OmopCloudEtlError):
    """Errors that occur during the execution of SQL."""

    def __init__(
        self,
        message: str,
        failed_sql: str,
        underlying_error: str,
        step_name: str,
        query_id: Optional[str] = None,
    ):
        super().__init__(
            f"Error in step '{step_name}' with query_id '{query_id}': "
            f"{message}\nUnderlying Error: {underlying_error}\nFailed SQL:\n{failed_sql}"
        )
        self.failed_sql = failed_sql
        self.underlying_error = underlying_error
        self.step_name = step_name
        self.query_id = query_id
