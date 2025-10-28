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
)

VALID_WORKFLOW = {
    "workflow_name": "test_workflow",
    "steps": [
        {"name": "load_data", "type": "bulk_load", "source_uri_pattern": "s3://bucket/data", "target_table": "person", "target_schema_ref": "cdm"},
        {"name": "transform_person", "type": "dml", "dml_file": "person.dml", "depends_on": ["load_data"]},
    ],
}

def test_workflow_config_validation_success():
    """Tests that a valid workflow configuration is parsed correctly."""
    config = WorkflowConfig.model_validate(VALID_WORKFLOW)
    assert config.workflow_name == "test_workflow"
    assert isinstance(config.steps[0], BulkLoadWorkflowStep)
    assert isinstance(config.steps[1], DMLWorkflowStep)
    assert config.steps[1].depends_on == ["load_data"]

def test_workflow_config_invalid_step_type():
    """Tests that a workflow with an invalid step type fails validation."""
    invalid_workflow = VALID_WORKFLOW.copy()
    invalid_workflow["steps"] = [{"name": "bad_step", "type": "unknown"}]
    with pytest.raises(ValidationError):
        WorkflowConfig.model_validate(invalid_workflow)

def test_compiled_workflow_plan_defaults():
    """Tests the default values of the CompiledWorkflowPlan."""
    plan = CompiledWorkflowPlan(workflow_name="test", concurrency=1, steps=[], context_snapshot={})
    assert plan.execution_id is not None
