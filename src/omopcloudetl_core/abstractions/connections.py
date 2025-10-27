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
