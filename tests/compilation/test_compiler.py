# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from omopcloudetl_core.compilation.compiler import WorkflowCompiler
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.models.workflow import WorkflowConfig
import yaml
from pydantic import ValidationError
from omopcloudetl_core.exceptions import (
    WorkflowError,
    CompilationError,
    DMLValidationError,
)
from omopcloudetl_core.specifications.models import CDMSpecification, CDMTableSpec


@pytest.fixture
def mock_project_config():
    """Provides a mock ProjectConfig."""
    return MagicMock(spec=ProjectConfig, schemas={"source": "raw", "cdm": "cdm_schema"})


@pytest.fixture
def mock_connection():
    """Provides a mock BaseConnection."""
    return MagicMock()


@pytest.fixture
def compiler(mock_project_config, mock_connection):
    """Provides a WorkflowCompiler instance with mocked dependencies."""
    with (
        patch("omopcloudetl_core.discovery.DiscoveryManager") as mock_dm,
        patch("omopcloudetl_core.compilation.compiler.SpecificationManager") as mock_sm,
    ):
        mock_sql_gen = MagicMock()
        mock_ddl_gen = MagicMock()
        mock_dm.return_value.get_generators.return_value = (mock_sql_gen, mock_ddl_gen)

        compiler_instance = WorkflowCompiler(mock_project_config, mock_connection)
        compiler_instance.sql_generator = mock_sql_gen
        compiler_instance.ddl_generator = mock_ddl_gen
        compiler_instance.spec_manager = mock_sm.return_value
        return compiler_instance


def test_dag_validation_success(compiler):
    """Tests that a valid DAG passes validation."""
    config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [
                {"name": "a", "type": "sql", "sql_file": "a.sql"},
                {"name": "b", "type": "sql", "sql_file": "b.sql", "depends_on": ["a"]},
            ],
        }
    )
    compiler._validate_dag(config.steps)


def test_dag_validation_cycle_error(compiler):
    """Tests that a DAG with a cycle raises a WorkflowError."""
    config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [
                {"name": "a", "type": "sql", "sql_file": "a.sql", "depends_on": ["b"]},
                {"name": "b", "type": "sql", "sql_file": "b.sql", "depends_on": ["a"]},
            ],
        }
    )
    with pytest.raises(WorkflowError, match="cycle"):
        compiler._validate_dag(config.steps)


def test_dag_validation_undefined_dependency(compiler):
    """Tests that a DAG with an undefined dependency raises a WorkflowError."""
    config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [{"name": "b", "type": "sql", "sql_file": "b.sql", "depends_on": ["a"]}],
        }
    )
    with pytest.raises(WorkflowError, match="undefined dependency"):
        compiler._validate_dag(config.steps)


@pytest.mark.parametrize(
    "side_effect, patch_target",
    [
        (IOError("File not found"), "builtins.open"),
        (
            ValidationError.from_exception_data("Validation error", []),
            "omopcloudetl_core.models.dml.DMLDefinition.model_validate",
        ),
        (yaml.YAMLError("YAML error"), "yaml.safe_load"),
    ],
)
def test_compile_dml_step_handles_errors(side_effect, patch_target, compiler):
    """Tests that DMLValidationError is raised for various errors during DML file processing."""
    workflow_config = WorkflowConfig.model_validate(
        {"workflow_name": "test", "steps": [{"name": "s1", "type": "dml", "dml_file": "a.dml"}]}
    )
    with patch(patch_target, side_effect=side_effect):
        with patch("builtins.open", mock_open(read_data="key: value")):  # Ensure open works for non-IOError tests
            with pytest.raises(DMLValidationError, match="Failed to load, render, or validate DML file"):
                compiler.compile(workflow_config, Path("."))


def test_compile_ddl_step(compiler):
    """Tests the successful compilation of a DDL step."""
    workflow_config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [{"name": "s1", "type": "ddl", "cdm_version": "5.4", "target_schema_ref": "cdm"}],
        }
    )
    mock_spec = CDMSpecification(
        version="5.4", tables={"person": CDMTableSpec(name="person", fields=[], primary_key=[])}
    )
    with patch.object(compiler.spec_manager, "fetch_specification", return_value=mock_spec):
        plan = compiler.compile(workflow_config, Path("."))
        assert len(plan.steps) == 1
        compiler.ddl_generator.generate_ddl.assert_called_once()


def test_compile_bulk_load_step(compiler):
    """Tests the successful compilation of a Bulk Load step."""
    workflow_config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [
                {
                    "name": "s1",
                    "type": "bulk_load",
                    "source_uri_pattern": "s3://bucket/{{ schemas.source }}",
                    "target_table": "person",
                    "target_schema_ref": "cdm",
                    "options": {
                        "source_format_options": {"delimiter": ","},
                        "load_options": {"skip_header": True},
                    },
                }
            ],
        }
    )
    plan = compiler.compile(workflow_config, Path("."))
    assert len(plan.steps) == 1
    compiled_step = plan.steps[0]
    assert compiled_step.type == "bulk_load"
    assert compiled_step.source_uri == "s3://bucket/raw"
    assert compiled_step.source_format_options == {"delimiter": ","}
    assert compiled_step.load_options == {"skip_header": True}


def test_compile_missing_schema_ref(compiler):
    """Tests that a CompilationError is raised for a missing schema reference."""
    workflow_config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [{"name": "s1", "type": "ddl", "cdm_version": "5.4", "target_schema_ref": "unknown"}],
        }
    )
    with pytest.raises(CompilationError, match="Schema reference 'unknown' not found"):
        compiler.compile(workflow_config, Path("."))


@patch("builtins.open", side_effect=IOError("File not found"))
def test_compile_sql_step_io_error(mock_open, compiler):
    """Tests that CompilationError is raised on an IOError during SQL file read."""
    workflow_config = WorkflowConfig.model_validate(
        {"workflow_name": "test", "steps": [{"name": "s1", "type": "sql", "sql_file": "a.sql"}]}
    )
    with pytest.raises(CompilationError, match="Failed to read SQL file"):
        compiler.compile(workflow_config, Path("."))


def test_compile_bulk_load_step_missing_schema(compiler):
    """Tests that a CompilationError is raised for a missing schema reference in a bulk load step."""
    workflow_config = WorkflowConfig.model_validate(
        {
            "workflow_name": "test",
            "steps": [
                {
                    "name": "s1",
                    "type": "bulk_load",
                    "source_uri_pattern": "s3://bucket/{{ schemas.source }}",
                    "target_table": "person",
                    "target_schema_ref": "unknown",
                }
            ],
        }
    )
    with pytest.raises(CompilationError, match="Schema reference 'unknown' not found"):
        compiler.compile(workflow_config, Path("."))


def test_compile_sql_step_success(compiler):
    """Tests the successful compilation of an SQL step."""
    with patch("builtins.open", mock_open(read_data="SELECT 1;")) as mock_file:
        workflow_config = WorkflowConfig.model_validate(
            {
                "workflow_name": "test_workflow",
                "steps": [{"name": "s1", "type": "sql", "sql_file": "a.sql"}],
            }
        )
        plan = compiler.compile(workflow_config, Path("."))
        assert len(plan.steps) == 1
        sql_step = plan.steps[0]
        assert sql_step.type == "sql"
        mock_file.assert_called_once_with(Path("./a.sql"), "r")

        # Verify the query tag is applied correctly
        tagged_sql = sql_step.sql_statements[0]
        assert tagged_sql.startswith("/* OmopCloudEtlContext:")
        assert '"omopcloudetl_tool": "WorkflowCompiler"' in tagged_sql
        assert '"step_name": "s1"' in tagged_sql
        assert '"workflow_name": "test_workflow"' in tagged_sql


def test_compile_sql_step_multiple_statements(compiler):
    """Tests that an SQL file with multiple statements is split correctly."""
    sql_content = "SELECT 1; SELECT 2;"
    with patch("builtins.open", mock_open(read_data=sql_content)):
        workflow_config = WorkflowConfig.model_validate(
            {"workflow_name": "test", "steps": [{"name": "s1", "type": "sql", "sql_file": "a.sql"}]}
        )
        plan = compiler.compile(workflow_config, Path("."))
        assert len(plan.steps[0].sql_statements) == 2


def test_compile_sql_step_empty_file(compiler):
    """Tests that an empty SQL file results in zero SQL statements."""
    with patch("builtins.open", mock_open(read_data="")):
        workflow_config = WorkflowConfig.model_validate(
            {"workflow_name": "test", "steps": [{"name": "s1", "type": "sql", "sql_file": "a.sql"}]}
        )
        plan = compiler.compile(workflow_config, Path("."))
        assert len(plan.steps[0].sql_statements) == 0


def test_jinja_rendering_undefined_variable_dml(compiler):
    """Tests that an undefined Jinja variable in a DML file raises an error."""
    with patch("builtins.open", mock_open(read_data="target_table: {{ tables.undefined }}")):
        workflow_config = WorkflowConfig.model_validate(
            {"workflow_name": "test", "steps": [{"name": "s1", "type": "dml", "dml_file": "a.dml"}]}
        )
        with pytest.raises(DMLValidationError):
            compiler.compile(workflow_config, Path("."))


def test_jinja_rendering_undefined_variable_sql(compiler):
    """Tests that an undefined Jinja variable in a SQL file raises an error."""
    with patch("builtins.open", mock_open(read_data="SELECT * FROM {{ schemas.undefined }}.table")):
        workflow_config = WorkflowConfig.model_validate(
            {"workflow_name": "test", "steps": [{"name": "s1", "type": "sql", "sql_file": "a.sql"}]}
        )
        # The underlying error is from Jinja, but we wrap it.
        # Let's check for our wrapper exception.
        with pytest.raises(CompilationError):
            compiler.compile(workflow_config, Path("."))


def test_compile_dml_step_success(compiler):
    """Tests the successful compilation of a DML step."""
    with patch(
        "builtins.open",
        mock_open(
            read_data="target_table: person\ntarget_schema_ref: cdm\nidempotency_keys: [person_id]\nprimary_source:\n  table: staging_person\n  alias: sp\n  schema_ref: source\nmappings:\n- type: direct\n  target_field: person_id\n  source_field: sp.person_id"
        ),
    ):
        workflow_config = WorkflowConfig.model_validate(
            {"workflow_name": "test", "steps": [{"name": "s1", "type": "dml", "dml_file": "a.dml"}]}
        )
        plan = compiler.compile(workflow_config, Path("."))
        assert len(plan.steps) == 1
        assert plan.steps[0].type == "sql"
        compiler.sql_generator.generate_transform_sql.assert_called_once()


def test_compile_empty_workflow(compiler):
    """Tests that a workflow with no steps compiles correctly."""
    workflow_config = WorkflowConfig.model_validate(
        {"workflow_name": "empty_workflow", "steps": []}
    )
    plan = compiler.compile(workflow_config, Path("."))
    assert len(plan.steps) == 0
    assert plan.workflow_name == "empty_workflow"
