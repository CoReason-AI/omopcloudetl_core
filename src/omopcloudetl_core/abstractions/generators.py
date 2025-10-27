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
from typing import Any, Dict, List

from omopcloudetl_core.models.dml import DMLDefinition
from omopcloudetl_core.specifications.models import CDMSpecification


class BaseDDLGenerator(ABC):
    """Abstract base class for dialect-specific DDL generators."""

    @abstractmethod
    def generate_ddl(self, specification: CDMSpecification, schema_name: str, options: Dict[str, Any]) -> List[str]:  # pragma: no cover
        """
        Generates a list of DDL statements for a given CDM specification.

        Args:
            specification: The parsed CDMSpecification object.
            schema_name: The target schema for the DDL.
            options: Dialect-specific options for DDL generation.

        Returns:
            A list of SQL DDL statements.
        """
        pass


class BaseSQLGenerator(ABC):
    """Abstract base class for dialect-specific SQL generators from DML definitions."""

    @abstractmethod
    def generate_transform_sql(self, dml_definition: DMLDefinition, context: Dict[str, Any]) -> str:  # pragma: no cover
        """
        Generates an idempotent SQL transformation statement from a DML definition.

        Args:
            dml_definition: The parsed DMLDefinition object.
            context: The execution context, containing resolved schema names, etc.

        Returns:
            A single, idempotent SQL statement (e.g., MERGE or a transactional block).
        """
        pass
