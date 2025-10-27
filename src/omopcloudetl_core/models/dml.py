# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any, Union, Literal, Annotated


# 1. Sources and Joins
class SourceTable(BaseModel):
    table: str
    alias: str
    # Reference to a schema defined in ProjectConfig (e.g., "source_schema")
    schema_ref: str


class Join(BaseModel):
    target: SourceTable
    on_condition: str  # SQL expression
    type: Literal["inner", "left", "right", "full"] = "left"


# 2. Mappings (Discriminated Union)
class BaseMapping(BaseModel):
    target_field: str


class DirectMapping(BaseMapping):
    type: Literal["direct"] = "direct"
    source_field: str  # e.g., "p.patient_id"


class ExpressionMapping(BaseMapping):
    type: Literal["expression"] = "expression"
    sql: str  # e.g., "COALESCE(p.gender, 0)"


class ConstantMapping(BaseMapping):
    type: Literal["constant"] = "constant"
    value: Any


MappingDefinition = Annotated[
    Union[DirectMapping, ExpressionMapping, ConstantMapping], Field(discriminator="type")
]

# 3. Root Definition
class DMLDefinition(BaseModel):
    target_table: str
    target_schema_ref: str

    # CRITICAL: Keys used for idempotency (MERGE matching or DELETE lookup)
    idempotency_keys: List[str]

    primary_source: SourceTable
    joins: List[Join] = Field(default_factory=list)
    where_clause: Optional[str] = None
    mappings: List[MappingDefinition]
