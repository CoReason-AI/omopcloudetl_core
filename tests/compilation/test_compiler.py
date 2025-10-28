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
from unittest.mock import MagicMock, mock_open, patch
from omopcloudetl_core.compilation.compiler import WorkflowCompiler
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.models.workflow import WorkflowConfig
from omopcloudetl_core.exceptions import WorkflowError, CompilationError
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
    with patch('omopcloudetl_core.discovery.DiscoveryManager') as mock_dm, \
         patch('omopcloudetl_core.compilation.compiler.SpecificationManager') as mock_sm:
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
    config = WorkflowConfig.model_validate({
        "workflow_name": "test", "steps": [
            {"name": "a", "type": "sql", "sql_file": "a.sql"},
            {"name": "b", "type": "sql", "sql_file": "b.sql", "depends_on": ["a"]},
        ]
    })
    compiler._validate_dag(config.steps)

def test_dag_validation_cycle_error(compiler):
    """Tests that a DAG with a cycle raises a WorkflowError."""
    config = WorkflowConfig.model_validate({
        "workflow_name": "test", "steps": [
            {"name": "a", "type": "sql", "sql_file": "a.sql", "depends_on": ["b"]},
            {"name": "b", "type": "sql", "sql_file": "b.sql", "depends_on": ["a"]},
        ]
    })
    with pytest.raises(WorkflowError, match="cycle"):
        compiler._validate_dag(config.steps)

def test_compile_ddl_step(compiler):
    """Tests the successful compilation of a DDL step."""
    workflow_config = WorkflowConfig.model_validate({
        "workflow_name": "test", "steps": [{"name": "s1", "type": "ddl", "cdm_version": "5.4", "target_schema_ref": "cdm"}]
    })
    mock_spec = CDMSpecification(version="5.4", tables={"person": CDMTableSpec(name="person", fields=[], primary_key=[])})
    with patch.object(compiler.spec_manager, 'fetch_specification', return_value=mock_spec):
        plan = compiler.compile(workflow_config, Path("."))
        assert len(plan.steps) == 1
        compiler.ddl_generator.generate_ddl.assert_called_once()

def test_compile_bulk_load_step(compiler):
    """Tests the successful compilation of a Bulk Load step."""
    workflow_config = WorkflowConfig.model_validate({
        "workflow_name": "test", "steps": [{
            "name": "s1", "type": "bulk_load", "source_uri_pattern": "s3://bucket/{{ schemas.source }}",
            "target_table": "person", "target_schema_ref": "cdm"
        }]
    })
    plan = compiler.compile(workflow_config, Path("."))
    assert len(plan.steps) == 1
    compiled_step = plan.steps[0]
    assert compiled_step.type == "bulk_load"
    assert compiled_step.source_uri == "s3://bucket/raw"

def test_compile_missing_schema_ref(compiler):
    """Tests that a CompilationError is raised for a missing schema reference."""
    workflow_config = WorkflowConfig.model_validate({
        "workflow_name": "test", "steps": [{"name": "s1", "type": "ddl", "cdm_version": "5.4", "target_schema_ref": "unknown"}]
    })
    with pytest.raises(CompilationError, match="Schema reference 'unknown' not found"):
        compiler.compile(workflow_config, Path("."))
