# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CDMFieldSpec(BaseModel):
    """Defines the specification for a single field in a CDM table."""

    name: str
    type: str  # Generalized SQL type (e.g., BIGINT, VARCHAR(50))
    required: bool
    description: Optional[str] = None


class CDMTableSpec(BaseModel):
    """Defines the specification for a single table in the CDM."""

    name: str
    fields: List[CDMFieldSpec]
    primary_key: List[str]
    foreign_keys: List[Dict[str, Any]] = Field(default_factory=list)
    indexes: List[Dict[str, Any]] = Field(default_factory=list)
    # Centralized optimization hints (e.g., {"databricks": {"zorder_by": ["field"]}})
    optimizations: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class CDMSpecification(BaseModel):
    """The root model for a complete OMOP CDM specification."""

    version: str
    tables: Dict[str, CDMTableSpec]
