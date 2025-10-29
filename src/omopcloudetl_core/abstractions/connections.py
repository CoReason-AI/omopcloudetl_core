# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, TYPE_CHECKING, Type, Optional, Sequence

from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics

if TYPE_CHECKING:
    # Use forward references to avoid circular imports at runtime
    from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator
    from omopcloudetl_core.config.models import ConnectionConfig


class ScalabilityTier(Enum):
    """Defines the scalability characteristics of a database platform."""

    TIER_1_HORIZONTAL = 1  # e.g., Databricks, Redshift, Spark (Horizontal Scale-Out)
    TIER_3_SINGLE_NODE = 3  # e.g., Postgres, SQL Server (Single-Node)


class BaseConnection(ABC):
    """Abstract base class for database connections."""

    # Concrete implementations of this class MUST define these class attributes
    SQL_GENERATOR_CLASS: Type["BaseSQLGenerator"]
    DDL_GENERATOR_CLASS: Type["BaseDDLGenerator"]

    def __init__(self, config: "ConnectionConfig"):
        self.config = config

    @property
    @abstractmethod
    def provider_type(self) -> str:  # pragma: no cover
        """Return the unique identifier for the database provider (e.g., 'databricks')."""
        pass

    @property
    @abstractmethod
    def scalability_tier(self) -> ScalabilityTier:  # pragma: no cover
        """Return the scalability tier of the database platform."""
        pass

    # fmt: off
    @abstractmethod
    def connect(self) -> None:  # pragma: no cover
        """
        Establishes the database connection.
        Implementations MUST use the tenacity library for connection retries.
        """
        pass
    # fmt: on

    @abstractmethod
    def fetch_data(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:  # pragma: no cover
        """
        Fetches data from the database using a SQL query.

        Args:
            sql: The SQL query to execute.
            params: An optional sequence of parameters for the query.

        Returns:
            The fetched data in a suitable format (e.g., a list of tuples or a DataFrame).
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str, schema_name: str) -> bool:  # pragma: no cover
        """
        Checks if a table exists in the database.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema.

        Returns:
            True if the table exists, False otherwise.
        """
        pass

    @abstractmethod
    def post_load_maintenance(self, table_name: str, schema_name: str) -> None:  # pragma: no cover
        """
        Performs post-load maintenance tasks, such as updating statistics or optimizing indexes.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema.
        """
        pass

    @abstractmethod
    def bulk_unload(
        self,
        target_uri: str,
        target_format: str,
        sql: str,
        unload_options: Optional[Dict[str, Any]] = None,
    ) -> None:  # pragma: no cover
        """
        Unloads data from a SQL query to a specified URI.

        Args:
            target_uri: The destination URI (e.g., 's3://bucket/path/').
            target_format: The format of the unloaded data (e.g., 'parquet', 'csv').
            sql: The SQL query to execute for the unload.
            unload_options: Additional options for the unload operation.
        """
        pass

    @abstractmethod
    def close(self) -> None:  # pragma: no cover
        """Closes the database connection."""
        pass

    # fmt: off
    @abstractmethod
    def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None, commit: bool = True
    ) -> ExecutionMetrics:  # pragma: no cover
        """
        Executes a SQL statement and returns structured execution metrics.

        Args:
            sql: The SQL statement to execute.
            params: An optional sequence of parameters to be safely substituted
                    into the SQL query by the database driver, preventing SQL injection.
            commit: If True, the transaction is committed.
        """
        pass
    # fmt: on

    # fmt: off
    @abstractmethod
    def bulk_load(
        self,
        source_uri: str,
        target_schema: str,
        target_table: str,
        source_format_options: Dict[str, Any],
        load_options: Dict[str, Any],
    ) -> LoadMetrics:  # pragma: no cover
        """
        Instructs the compute engine to load data directly from a cloud storage URI.

        Args:
            source_uri: The URI of the source data (e.g., 's3://bucket/path/').
                        This MUST NOT be a local file path.
            target_schema: The schema of the target table.
            target_table: The name of the target table.
            source_format_options: Options describing the source data format (e.g., {'format': 'csv', 'header': 'true'}).
            load_options: Additional options for the load operation (e.g., {'mode': 'overwrite'}).

        Returns:
            Structured load metrics.
        """
        pass
    # fmt: on
