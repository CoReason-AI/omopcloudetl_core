# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from omopcloudetl_core.compilation.compiler import WorkflowCompiler
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.exceptions import CompilationError, WorkflowError
from omopcloudetl_core.models.workflow import (
    CompiledBulkLoadStep,
    CompiledSQLStep,
    WorkflowConfig,
)

# A sample workflow definition for testing
WORKFLOW_DICT = {
    "workflow_name": "test_workflow",
    "steps": [
        {"name": "load_data", "type": "bulk_load", "source_uri_pattern": "/path/to/data", "target_table": "my_table", "target_schema_ref": "target"},
        {"name": "transform", "type": "dml", "dml_file": "transform.yml", "depends_on": ["load_data"]},
        {"name": "generate_ddl", "type": "ddl", "cdm_version": "5.4", "target_schema_ref": "cdm", "depends_on": []},
        {"name": "custom_sql", "type": "sql", "sql_file": "custom.sql", "depends_on": ["transform"]},
    ],
}


@pytest.fixture
def mock_dependencies():
    """Fixture to create mock dependencies for the compiler."""
    return {
        "project_config": ProjectConfig(
            connection={"provider_type": "mock"},
            orchestrator={"type": "mock"},
            schemas={"target": "resolved_target", "cdm": "resolved_cdm"},
        ),
        "sql_generator": MagicMock(),
        "ddl_generator": MagicMock(),
        "spec_manager": MagicMock(),
    }


@pytest.fixture
def compiler(mock_dependencies):
    """Fixture to create a WorkflowCompiler instance with mock dependencies."""
    return WorkflowCompiler(**mock_dependencies)


@pytest.fixture
def workflow_base_path(tmp_path: Path) -> Path:
    """Fixture to create a temporary directory with mock workflow files."""
    dml_path = tmp_path / "transform.yml"
    dml_path.write_text("target_table: person\ntarget_schema_ref: cdm\nidempotency_keys: [person_id]\nprimary_source: {table: stg, alias: s, schema_ref: src}\nmappings: []")

    sql_path = tmp_path / "custom.sql"
    sql_path.write_text("SELECT 1;")

    return tmp_path


def test_compile_success(compiler, workflow_base_path):
    """Test successful compilation of a valid workflow."""
    compiler.sql_generator.generate_transform_sql.return_value = "MERGE INTO person..."
    compiler.ddl_generator.generate_ddl.return_value = ["CREATE TABLE person...", "CREATE TABLE visit..."]

    workflow_config = WorkflowConfig(**deepcopy(WORKFLOW_DICT))
    plan = compiler.compile(workflow_config, workflow_base_path)

    assert plan.workflow_name == "test_workflow"
    assert len(plan.steps) == 4

    # Validate each compiled step type
    load_step = next(s for s in plan.steps if s.name == "load_data")
    assert isinstance(load_step, CompiledBulkLoadStep)
    assert load_step.target_schema == "resolved_target"

    transform_step = next(s for s in plan.steps if s.name == "transform")
    assert isinstance(transform_step, CompiledSQLStep)
    assert "MERGE" in transform_step.sql_statements[0]

    ddl_step = next(s for s in plan.steps if s.name == "generate_ddl")
    assert isinstance(ddl_step, CompiledSQLStep)
    assert len(ddl_step.sql_statements) == 2
    assert "CREATE TABLE" in ddl_step.sql_statements[0]

    sql_step = next(s for s in plan.steps if s.name == "custom_sql")
    assert isinstance(sql_step, CompiledSQLStep)
    assert "SELECT 1" in sql_step.sql_statements[0]


def test_dag_validation_cycle_error(compiler, tmp_path):
    """Test that a cycle in the workflow dependencies raises a WorkflowError."""
    cyclic_workflow = {
        "workflow_name": "cyclic",
        "steps": [
            {"name": "a", "type": "sql", "sql_file": "a.sql", "depends_on": ["c"]},
            {"name": "b", "type": "sql", "sql_file": "b.sql", "depends_on": ["a"]},
            {"name": "c", "type": "sql", "sql_file": "c.sql", "depends_on": ["b"]},
        ],
    }
    # Create dummy files
    (tmp_path / "a.sql").touch()
    (tmp_path / "b.sql").touch()
    (tmp_path / "c.sql").touch()

    workflow_config = WorkflowConfig(**cyclic_workflow)
    with pytest.raises(WorkflowError, match="contains cycles"):
        compiler.compile(workflow_config, tmp_path)


def test_dag_validation_missing_dependency_error(compiler, workflow_base_path):
    """Test that a missing dependency raises a WorkflowError."""
    missing_dep_workflow = deepcopy(WORKFLOW_DICT)
    missing_dep_workflow["steps"][1]["depends_on"] = ["non_existent_step"]
    workflow_config = WorkflowConfig(**missing_dep_workflow)
    with pytest.raises(WorkflowError, match="undefined dependency"):
        compiler.compile(workflow_config, workflow_base_path)


def test_compilation_error_on_missing_file(compiler, tmp_path):
    """Test that a CompilationError is raised if a DML or SQL file is missing."""
    workflow_config = WorkflowConfig(**deepcopy(WORKFLOW_DICT))
    # Use an empty directory as the base path, so transform.yml will be missing
    with pytest.raises(CompilationError, match="Failed to compile DML step"):
        compiler.compile(workflow_config, tmp_path)
