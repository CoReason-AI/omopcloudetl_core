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
from unittest.mock import MagicMock, patch, mock_open

import pytest
from omopcloudetl_core.compilation.compiler import WorkflowCompiler
from omopcloudetl_core.config.models import ConnectionConfig, ProjectConfig
from omopcloudetl_core.exceptions import DMLValidationError, WorkflowError
from omopcloudetl_core.models.workflow import (
    BulkLoadWorkflowStep,
    CompiledBulkLoadStep,
    CompiledSQLStep,
    DDLWorkflowStep,
    DMLWorkflowStep,
    SQLWorkflowStep,
    WorkflowConfig,
)

# A valid DML YAML content for mocking file reads, now with a Jinja variable
VALID_DML_CONTENT = """
target_table: person
target_schema_ref: {{ schemas.target }}
idempotency_keys: [person_id]
primary_source:
  table: stg_person
  alias: p
  schema_ref: source
mappings:
  - type: direct
    target_field: person_id
    source_field: p.person_id
"""


@pytest.fixture
def mock_project_config():
    """Fixture for a mock ProjectConfig."""
    return MagicMock(
        spec=ProjectConfig,
        schemas={"source": "raw_data", "target": "cdm_v5"},
        connection=MagicMock(spec=ConnectionConfig),
    )


@pytest.fixture
def mock_connection():
    """Fixture for a mock BaseConnection."""
    mock_conn = MagicMock()
    # Mock the generator classes on the instance
    mock_conn.SQL_GENERATOR_CLASS = MagicMock()
    mock_conn.DDL_GENERATOR_CLASS = MagicMock()
    return mock_conn


@patch("omopcloudetl_core.compilation.compiler.DiscoveryManager")
@patch("omopcloudetl_core.compilation.compiler.SpecificationManager")
def test_compiler_initialization(MockSpecManager, MockDiscoveryManager, mock_project_config, mock_connection):
    """Test that the compiler initializes its managers and generators correctly."""
    mock_sql_gen, mock_ddl_gen = MagicMock(), MagicMock()
    MockDiscoveryManager.return_value.get_generators.return_value = (mock_sql_gen, mock_ddl_gen)

    compiler = WorkflowCompiler(mock_project_config, mock_connection)

    assert compiler.discovery_manager is not None
    assert compiler.spec_manager is not None
    assert compiler.sql_generator is mock_sql_gen
    assert compiler.ddl_generator is mock_ddl_gen
    MockDiscoveryManager.return_value.get_generators.assert_called_once_with(mock_connection)


@patch("omopcloudetl_core.compilation.compiler.DiscoveryManager")
@patch("omopcloudetl_core.compilation.compiler.SpecificationManager")
def test_compile_dml_step(MockSpecManager, MockDiscoveryManager, mock_project_config, mock_connection):
    """Test the compilation of a DML workflow step."""
    mock_sql_gen = MagicMock()
    mock_sql_gen.generate_transform_sql.return_value = "MERGE INTO target_table ...;"
    MockDiscoveryManager.return_value.get_generators.return_value = (mock_sql_gen, MagicMock())

    compiler = WorkflowCompiler(mock_project_config, mock_connection)

    workflow_config = WorkflowConfig(
        workflow_name="test_wf",
        steps=[DMLWorkflowStep(name="transform_person", dml_file="person.yml")],
    )

    # Mock file reading with valid DML content
    m_open = mock_open(read_data=VALID_DML_CONTENT)
    with patch("builtins.open", m_open):
        plan = compiler.compile(workflow_config, Path("/workflows"))

    assert len(plan.steps) == 1
    compiled_step = plan.steps[0]
    assert isinstance(compiled_step, CompiledSQLStep)
    assert compiled_step.name == "transform_person"
    assert "MERGE INTO" in compiled_step.sql_statements[0]
    assert "omopcloudetl_tool" in compiled_step.sql_statements[0]
    m_open.assert_called_once_with(Path("/workflows/person.yml"), "r")
    mock_sql_gen.generate_transform_sql.assert_called_once()
    # Check that the Jinja variable was rendered
    rendered_dml_def = mock_sql_gen.generate_transform_sql.call_args[0][0]
    assert rendered_dml_def.target_schema_ref == "cdm_v5"


@patch("omopcloudetl_core.compilation.compiler.DiscoveryManager")
@patch("omopcloudetl_core.compilation.compiler.SpecificationManager")
def test_compile_ddl_step(MockSpecManager, MockDiscoveryManager, mock_project_config, mock_connection):
    """Test the compilation of a DDL workflow step."""
    mock_ddl_gen = MagicMock()
    mock_ddl_gen.generate_ddl.return_value = ["CREATE TABLE ...;", "ALTER TABLE ...;"]
    MockDiscoveryManager.return_value.get_generators.return_value = (MagicMock(), mock_ddl_gen)

    mock_spec = MagicMock()
    MockSpecManager.return_value.fetch_specification.return_value = mock_spec

    compiler = WorkflowCompiler(mock_project_config, mock_connection)
    workflow_config = WorkflowConfig(
        workflow_name="test_wf",
        steps=[DDLWorkflowStep(name="create_tables", cdm_version="5.4", target_schema_ref="target")],
    )

    plan = compiler.compile(workflow_config, Path("/workflows"))

    assert len(plan.steps) == 1
    compiled_step = plan.steps[0]
    assert isinstance(compiled_step, CompiledSQLStep)
    assert len(compiled_step.sql_statements) == 2
    assert "CREATE TABLE" in compiled_step.sql_statements[0]
    MockSpecManager.return_value.fetch_specification.assert_called_once_with("5.4")
    mock_ddl_gen.generate_ddl.assert_called_once_with(mock_spec, "cdm_v5", {})


@patch("omopcloudetl_core.compilation.compiler.DiscoveryManager")
@patch("omopcloudetl_core.compilation.compiler.SpecificationManager")
def test_compile_bulk_load_step(MockSpecManager, MockDiscoveryManager, mock_project_config, mock_connection):
    """Test the compilation of a BulkLoad workflow step."""
    MockDiscoveryManager.return_value.get_generators.return_value = (MagicMock(), MagicMock())
    compiler = WorkflowCompiler(mock_project_config, mock_connection)
    workflow_config = WorkflowConfig(
        workflow_name="test_wf",
        steps=[
            BulkLoadWorkflowStep(
                name="load_raw_data",
                source_uri_pattern="s3://bucket/{{ schemas.source }}/",
                target_table="person",
                target_schema_ref="source",
            )
        ],
    )
    plan = compiler.compile(workflow_config, Path("/workflows"))

    assert len(plan.steps) == 1
    compiled_step = plan.steps[0]
    assert isinstance(compiled_step, CompiledBulkLoadStep)
    assert compiled_step.name == "load_raw_data"
    assert compiled_step.target_schema == "raw_data"
    assert compiled_step.source_uri == "s3://bucket/raw_data/"


@patch("omopcloudetl_core.compilation.compiler.DiscoveryManager")
@patch("omopcloudetl_core.compilation.compiler.SpecificationManager")
def test_dag_validation_failure(MockSpecManager, MockDiscoveryManager, mock_project_config, mock_connection):
    """Test that compilation fails with an invalid (cyclic) DAG."""
    MockDiscoveryManager.return_value.get_generators.return_value = (MagicMock(), MagicMock())
    compiler = WorkflowCompiler(mock_project_config, mock_connection)
    workflow_config = WorkflowConfig(
        workflow_name="test_wf",
        steps=[
            SQLWorkflowStep(name="step_a", depends_on=["step_c"], sql_file="a.sql"),
            SQLWorkflowStep(name="step_b", depends_on=["step_a"], sql_file="b.sql"),
            SQLWorkflowStep(name="step_c", depends_on=["step_b"], sql_file="c.sql"),
        ],
    )

    with pytest.raises(WorkflowError, match="circular dependency"):
        compiler.compile(workflow_config, Path("/workflows"))


@patch("omopcloudetl_core.compilation.compiler.DiscoveryManager")
@patch("omopcloudetl_core.compilation.compiler.SpecificationManager")
def test_dml_validation_error(MockSpecManager, MockDiscoveryManager, mock_project_config, mock_connection):
    """Test that compilation fails with an invalid DML file."""
    MockDiscoveryManager.return_value.get_generators.return_value = (MagicMock(), MagicMock())
    compiler = WorkflowCompiler(mock_project_config, mock_connection)
    workflow_config = WorkflowConfig(
        workflow_name="test_wf",
        steps=[DMLWorkflowStep(name="bad_dml", dml_file="bad.yml")],
    )

    m_open = mock_open(read_data="this is not valid yaml!")
    with patch("builtins.open", m_open), pytest.raises(DMLValidationError):
        compiler.compile(workflow_config, Path("/workflows"))
