# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Sequence, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


# Part 1: Workflow Configuration (User-defined)
class BaseWorkflowStep(BaseModel):
    """Base model for a step in the user-defined workflow configuration."""

    name: str
    depends_on: List[str] = Field(default_factory=list)


class DMLWorkflowStep(BaseWorkflowStep):
    """A workflow step to execute a DML transformation from a file."""

    type: Literal["dml"] = "dml"
    dml_file: str


class BulkLoadWorkflowStep(BaseWorkflowStep):
    """A workflow step for bulk loading data from a cloud URI."""

    type: Literal["bulk_load"] = "bulk_load"
    source_uri_pattern: str
    target_table: str
    target_schema_ref: str
    options: Dict[str, Any] = Field(default_factory=dict)


class DDLWorkflowStep(BaseWorkflowStep):
    """A workflow step to generate and execute DDL for a specific CDM version."""

    type: Literal["ddl"] = "ddl"
    cdm_version: str
    target_schema_ref: str
    options: Dict[str, Any] = Field(default_factory=dict)


class SQLWorkflowStep(BaseWorkflowStep):
    """A workflow step for executing auxiliary SQL from a file (e.g., for QA)."""

    type: Literal["sql"] = "sql"
    sql_file: str

    @field_validator("sql_file")
    def must_be_relative_path(cls, v: str) -> str:
        """Validate that the SQL file is a relative path."""
        if Path(v).is_absolute():
            raise ValueError("SQL file path must be relative.")
        return v


WorkflowStep = Annotated[
    Union[DMLWorkflowStep, BulkLoadWorkflowStep, DDLWorkflowStep, SQLWorkflowStep],
    Field(discriminator="type"),
]


class WorkflowConfig(BaseModel):
    """The root model for a user-defined workflow configuration."""

    workflow_name: str
    concurrency: int = 1
    steps: List[WorkflowStep]


# Part 2: Compiled Workflow Plan (Compiler Output)
class CompiledBaseStep(BaseModel):
    """Base model for a step in the compiled, executable workflow plan."""

    name: str
    depends_on: List[str]


class CompiledSQLStep(CompiledBaseStep):
    """A compiled step containing one or more SQL statements to be executed."""

    type: Literal["sql"] = "sql"
    sql_statements: List[str]


class CompiledBulkLoadStep(CompiledBaseStep):
    """A compiled step containing resolved parameters for a bulk load operation."""

    type: Literal["bulk_load"] = "bulk_load"
    source_uri: str
    target_schema: str
    target_table: str
    source_format_options: Dict[str, Any]
    load_options: Dict[str, Any]


CompiledStep = Annotated[Union[CompiledSQLStep, CompiledBulkLoadStep], Field(discriminator="type")]


class CompiledWorkflowPlan(BaseModel):
    """
    The executable artifact produced by the WorkflowCompiler. This is the
    input for an orchestrator.
    """

    execution_id: UUID = Field(default_factory=uuid4)
    workflow_name: str
    concurrency: int
    steps: Sequence[CompiledStep]
    context_snapshot: Dict[str, Any]
