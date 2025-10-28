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
from unittest.mock import MagicMock, patch
from omopcloudetl_core.compilation.compiler import WorkflowCompiler
from omopcloudetl_core.models.workflow import WorkflowConfig
from omopcloudetl_core.exceptions import WorkflowError, CompilationError, DMLValidationError


@pytest.fixture
def mock_project_config():
    """Fixture for a mock ProjectConfig."""
    config = MagicMock()
    config.schemas = {"source": "my_source", "target": "my_target"}
    return config


@pytest.fixture
def mock_connection():
    """Fixture for a mock BaseConnection."""
    return MagicMock()


@pytest.fixture
def compiler(mock_project_config, mock_connection):
    """Fixture for a WorkflowCompiler."""
    with patch("omopcloudetl_core.discovery.DiscoveryManager") as mock_discovery:
        mock_sql_gen = MagicMock()
        mock_ddl_gen = MagicMock()
        mock_discovery.return_value.get_generators.return_value = (mock_sql_gen, mock_ddl_gen)

        compiler_instance = WorkflowCompiler(mock_project_config, mock_connection)
        compiler_instance.sql_generator = mock_sql_gen
        compiler_instance.ddl_generator = mock_ddl_gen
        return compiler_instance


def test_dag_validation_success(compiler):
    """Test that a valid DAG passes validation."""
    step1 = MagicMock()
    step1.name = "step1"
    step1.depends_on = []

    step2 = MagicMock()
    step2.name = "step2"
    step2.depends_on = ["step1"]

    steps = [step1, step2]
    compiler._validate_dag(steps)


def test_dag_validation_cycle_failure(compiler):
    """Test that a DAG with a cycle fails validation."""
    steps = [
        MagicMock(name="step1", depends_on=["step2"]),
        MagicMock(name="step2", depends_on=["step1"]),
    ]
    with pytest.raises(WorkflowError):
        compiler._validate_dag(steps)


def test_dag_validation_missing_dependency_failure(compiler):
    """Test that a DAG with a missing dependency fails validation."""
    steps = [MagicMock(name="step2", depends_on=["step1"])]
    with pytest.raises(WorkflowError):
        compiler._validate_dag(steps)


def test_compile_sql_step_success(compiler, tmp_path):
    """Test successful compilation of a SQL step."""
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM {{ schemas.source }}.my_table;")

    workflow_config = WorkflowConfig(
        workflow_name="test_workflow", steps=[{"type": "sql", "name": "run_sql", "sql_file": "test.sql"}]
    )

    plan = compiler.compile(workflow_config, tmp_path)

    assert len(plan.steps) == 1
    compiled_step = plan.steps[0]
    assert compiled_step.name == "run_sql"
    assert compiled_step.type == "sql"
    assert "SELECT * FROM my_source.my_table;" in compiled_step.sql_statements[0]
    assert "OmopCloudEtlContext" in compiled_step.sql_statements[0]


def test_compile_file_not_found_failure(compiler, tmp_path):
    """Test that compilation fails if a step's file is not found."""
    workflow_config = WorkflowConfig(
        workflow_name="test_workflow", steps=[{"type": "sql", "name": "run_sql", "sql_file": "non_existent.sql"}]
    )
    with pytest.raises(CompilationError):
        compiler.compile(workflow_config, tmp_path)


@pytest.mark.parametrize(
    "invalid_content, expected_exception",
    [
        ("not a list", DMLValidationError),
        ("- item1\n- item2", DMLValidationError),
        ("key: value\n  sub_key: value", DMLValidationError),
    ],
)
def test_compile_dml_step_invalid_content(compiler, tmp_path, invalid_content, expected_exception):
    """Test compilation of a DML step with invalid YAML content."""
    dml_file = tmp_path / "invalid.dml.yml"
    dml_file.write_text(invalid_content)

    workflow_config = WorkflowConfig(
        workflow_name="test_workflow", steps=[{"type": "dml", "name": "run_dml", "dml_file": "invalid.dml.yml"}]
    )

    with pytest.raises(expected_exception):
        compiler.compile(workflow_config, tmp_path)
