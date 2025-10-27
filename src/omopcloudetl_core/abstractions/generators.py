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
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from omopcloudetl_core.models.dml import DMLDefinition
    from omopcloudetl_core.specifications.models import CDMSpecification


class BaseDDLGenerator(ABC):
    """Abstract base class for DDL generators."""

    @abstractmethod
    def generate_ddl(
        self, specification: "CDMSpecification", schema_name: str, options: dict
    ) -> List[str]:  # pragma: no cover
        """Generates DDL statements from a CDM specification."""
        pass


class BaseSQLGenerator(ABC):
    """Abstract base class for SQL generators."""

    @abstractmethod
    def generate_transform_sql(
        self, dml_definition: "DMLDefinition", context: Dict[str, Any]
    ) -> str:  # pragma: no cover
        """Generates idempotent transformation SQL from a DML definition."""
        pass
