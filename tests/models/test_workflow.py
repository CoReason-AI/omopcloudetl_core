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
from pydantic import ValidationError
from omopcloudetl_core.models.workflow import (
    WorkflowConfig,
    DMLWorkflowStep,
    BulkLoadWorkflowStep,
    CompiledWorkflowPlan,
    CompiledSQLStep,
)

VALID_WORKFLOW_CONFIG_DICT = {
    "workflow_name": "my_etl_workflow",
    "concurrency": 4,
    "steps": [
        {
            "name": "load_patients",
            "type": "bulk_load",
            "source_uri_pattern": "s3://my-bucket/patients/*.csv",
            "target_table": "stg_patients",
            "target_schema_ref": "source_schema",
        },
        {
            "name": "transform_patients",
            "type": "dml",
            "dml_file": "dml/transform_patients.yml",
            "depends_on": ["load_patients"],
        },
        {"name": "create_cdm", "type": "ddl", "cdm_version": "5.4", "target_schema_ref": "cdm_schema"},
    ],
}


def test_workflow_config_valid():
    """Test successful parsing of a valid workflow configuration."""
    config = WorkflowConfig(**VALID_WORKFLOW_CONFIG_DICT)

    assert config.workflow_name == "my_etl_workflow"
    assert config.concurrency == 4
    assert len(config.steps) == 3

    # Test discriminated union parsing
    assert isinstance(config.steps[0], BulkLoadWorkflowStep)
    assert config.steps[0].target_table == "stg_patients"

    assert isinstance(config.steps[1], DMLWorkflowStep)
    assert config.steps[1].depends_on == ["load_patients"]


def test_workflow_config_invalid_step_type():
    """Test that validation fails for an unknown step type."""
    invalid_dict = VALID_WORKFLOW_CONFIG_DICT.copy()
    invalid_dict["steps"] = [{"name": "bad_step", "type": "unknown"}]
    with pytest.raises(ValidationError):
        WorkflowConfig(**invalid_dict)


def test_compiled_workflow_plan_valid():
    """Test successful creation of a compiled workflow plan."""
    plan_dict = {
        "workflow_name": "compiled_workflow",
        "concurrency": 2,
        "steps": [
            {
                "name": "compiled_sql_step",
                "type": "sql",
                "depends_on": [],
                "sql_statements": ["SELECT 1;", "INSERT INTO my_table VALUES (1);"],
            },
            {
                "name": "compiled_load_step",
                "type": "bulk_load",
                "depends_on": ["compiled_sql_step"],
                "source_uri": "s3://my-bucket/data.csv",
                "target_schema": "my_schema",
                "target_table": "my_table",
                "source_format_options": {},
                "load_options": {},
            },
        ],
        "context_snapshot": {"schema_ref": "resolved_schema"},
    }
    plan = CompiledWorkflowPlan(**plan_dict)

    assert plan.workflow_name == "compiled_workflow"
    assert len(plan.steps) == 2
    assert isinstance(plan.steps[0], CompiledSQLStep)
    assert len(plan.steps[0].sql_statements) == 2
    assert plan.context_snapshot["schema_ref"] == "resolved_schema"


def test_compiled_workflow_plan_generates_uuid():
    """Test that a UUID is automatically generated for the execution_id."""
    plan = CompiledWorkflowPlan(workflow_name="test", concurrency=1, steps=[], context_snapshot={})
    assert plan.execution_id is not None
