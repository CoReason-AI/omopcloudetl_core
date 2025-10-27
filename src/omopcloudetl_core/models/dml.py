# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Annotated, Any, List, Literal, Optional, Union

from pydantic import BaseModel, Field


# 1. Sources and Joins
class SourceTable(BaseModel):
    """Defines a source table with its alias and a reference to its schema."""

    table: str
    alias: str
    schema_ref: str  # Reference to a key in ProjectConfig.schemas


class Join(BaseModel):
    """Defines a join operation between tables."""

    target: SourceTable
    on_condition: str  # SQL expression for the join condition
    type: Literal["inner", "left", "right", "full"] = "left"


# 2. Mappings (Discriminated Union)
class BaseMapping(BaseModel):
    """Base model for all mapping types."""

    target_field: str


class DirectMapping(BaseMapping):
    """Maps a source field directly to a target field."""

    type: Literal["direct"] = "direct"
    source_field: str  # e.g., "p.patient_id"


class ExpressionMapping(BaseMapping):
    """Maps an SQL expression to a target field."""

    type: Literal["expression"] = "expression"
    sql: str  # e.g., "COALESCE(p.gender, 0)"


class ConstantMapping(BaseMapping):
    """Maps a constant value to a target field."""

    type: Literal["constant"] = "constant"
    value: Any


MappingDefinition = Annotated[
    Union[DirectMapping, ExpressionMapping, ConstantMapping],
    Field(discriminator="type"),
]


# 3. Root Definition
class DMLDefinition(BaseModel):
    """The root model for a declarative DML definition."""

    target_table: str
    target_schema_ref: str

    # Keys used for idempotency (e.g., for MERGE or DELETE+INSERT)
    idempotency_keys: List[str]

    primary_source: SourceTable
    joins: List[Join] = Field(default_factory=list)
    where_clause: Optional[str] = None
    mappings: List[MappingDefinition]
