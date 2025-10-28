# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Annotated, Any, Dict, List, Literal, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# Part 1: User-Defined Workflow Configuration Steps
class BaseWorkflowStep(BaseModel):
    """Base model for a step in a user-defined workflow."""

    name: str
    depends_on: List[str] = Field(default_factory=list)


class DMLWorkflowStep(BaseWorkflowStep):
    """Workflow step for executing a DML transformation from a file."""

    type: Literal["dml"] = "dml"
    dml_file: str


class BulkLoadWorkflowStep(BaseWorkflowStep):
    """Workflow step for bulk loading data from a cloud storage URI."""

    type: Literal["bulk_load"] = "bulk_load"
    source_uri_pattern: str
    target_table: str
    target_schema_ref: str
    options: Dict[str, Any] = Field(default_factory=dict)


class DDLWorkflowStep(BaseWorkflowStep):
    """Workflow step for executing DDL based on a CDM specification."""

    type: Literal["ddl"] = "ddl"
    cdm_version: str
    target_schema_ref: str
    options: Dict[str, Any] = Field(default_factory=dict)


class SQLWorkflowStep(BaseWorkflowStep):
    """Workflow step for executing an auxiliary SQL script."""

    type: Literal["sql"] = "sql"
    sql_file: str


WorkflowStep = Annotated[
    Union[DMLWorkflowStep, BulkLoadWorkflowStep, DDLWorkflowStep, SQLWorkflowStep],
    Field(discriminator="type"),
]


# Part 2: User-Defined Workflow Configuration Root
class WorkflowConfig(BaseModel):
    """Root model for a user-defined workflow configuration."""

    workflow_name: str
    concurrency: int = 1
    steps: List[WorkflowStep]


# Part 3: Compiled Workflow Plan (The Executable Artifact)
class CompiledBaseStep(BaseModel):
    """Base model for a step in a compiled workflow plan."""

    name: str
    depends_on: List[str]


class CompiledSQLStep(CompiledBaseStep):
    """A compiled step that contains fully rendered SQL statements."""

    type: Literal["sql"] = "sql"
    sql_statements: List[str]


class CompiledBulkLoadStep(CompiledBaseStep):
    """A compiled step for a bulk load operation with resolved parameters."""

    type: Literal["bulk_load"] = "bulk_load"
    source_uri: str
    target_schema: str
    target_table: str
    source_format_options: Dict[str, Any]
    load_options: Dict[str, Any]


CompiledStep = Annotated[
    Union[CompiledSQLStep, CompiledBulkLoadStep],
    Field(discriminator="type"),
]


class CompiledWorkflowPlan(BaseModel):
    """The final, executable artifact produced by the WorkflowCompiler."""

    execution_id: UUID = Field(default_factory=uuid4)
    workflow_name: str
    concurrency: int
    steps: List[CompiledStep]
    context_snapshot: Dict[str, Any]
