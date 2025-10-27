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
from typing import Dict, Any, Type, TYPE_CHECKING

from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics

if TYPE_CHECKING:
    # Avoid circular imports at runtime
    from .generators import BaseSQLGenerator, BaseDDLGenerator


class ScalabilityTier(Enum):
    """Used by the CLI to advise on platform suitability."""

    TIER_1_HORIZONTAL = 1  # e.g., Databricks, Redshift, Spark (Horizontal Scale-Out)
    TIER_3_SINGLE_NODE = 3  # e.g., Postgres, SQL Server (Single-Node)


class BaseConnection(ABC):
    """Abstract base class for database connections."""

    # Convention: Implementations MUST define these class attributes
    SQL_GENERATOR_CLASS: Type["BaseSQLGenerator"]
    DDL_GENERATOR_CLASS: Type["BaseDDLGenerator"]

    @property
    @abstractmethod
    def provider_type(self) -> str:  # pragma: no cover
        """Returns the type of the database provider."""
        pass

    @property
    @abstractmethod
    def scalability_tier(self) -> ScalabilityTier:  # pragma: no cover
        """Returns the scalability tier of the database provider."""
        pass

    @abstractmethod
    def connect(self):  # pragma: no cover
        """Establishes connection. Implementations MUST use tenacity for retries."""
        pass

    @abstractmethod
    def close(self):  # pragma: no cover
        """Closes the database connection."""
        pass

    @abstractmethod
    def execute_sql(self, sql: str, commit: bool = True) -> ExecutionMetrics:  # pragma: no cover
        """Executes SQL and returns structured execution metrics."""
        pass

    @abstractmethod
    def bulk_load(  # pragma: no cover
        self,
        source_uri: str,  # Must be URI (s3://, abfss://), not local path
        target_schema: str,
        target_table: str,
        source_format_options: Dict[str, Any],
        load_options: Dict[str, Any],
    ) -> LoadMetrics:
        """Instructs the compute engine to load data directly from the source URI."""
        pass
