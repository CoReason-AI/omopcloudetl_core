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
from typing import List, Dict, Union, Literal, Annotated, Any
from uuid import UUID


# Define Compiled Steps (Executable artifacts) - PLACEHOLDERS
class CompiledBaseStep(BaseModel):
    name: str
    depends_on: List[str]


class CompiledSQLStep(CompiledBaseStep):
    type: Literal["sql"] = "sql"
    sql_statements: List[str]


class CompiledBulkLoadStep(CompiledBaseStep):
    type: Literal["bulk_load"] = "bulk_load"
    source_uri: str
    target_schema: str
    target_table: str
    source_format_options: Dict
    load_options: Dict


CompiledStep = Annotated[Union[CompiledSQLStep, CompiledBulkLoadStep], Field(discriminator="type")]


class CompiledWorkflowPlan(BaseModel):
    """A placeholder implementation of the compiled workflow plan."""

    execution_id: UUID
    workflow_name: str
    concurrency: int
    steps: List[CompiledStep]
    context_snapshot: Dict[str, Any]
