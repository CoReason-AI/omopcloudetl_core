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
from omopcloudetl_core.abstractions.orchestrators import BaseOrchestrator, ExecutionResult
from omopcloudetl_core.models.workflow import CompiledWorkflowPlan
import uuid


class ConcreteOrchestrator(BaseOrchestrator):
    """A minimal concrete implementation of BaseOrchestrator for testing."""

    def execute_plan(
        self, plan: "CompiledWorkflowPlan", dry_run: bool = False, resume: bool = False
    ) -> ExecutionResult:
        return ExecutionResult(success=True, message="Plan executed successfully.")


def test_base_orchestrator_cannot_be_instantiated():
    """Test that the abstract BaseOrchestrator cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseOrchestrator()


def test_concrete_orchestrator_can_be_instantiated():
    """Test that a concrete subclass of BaseOrchestrator can be instantiated."""
    orchestrator = ConcreteOrchestrator()
    assert isinstance(orchestrator, BaseOrchestrator)


def test_concrete_orchestrator_execute_plan():
    """Test that the execute_plan method of a concrete orchestrator is callable."""
    orchestrator = ConcreteOrchestrator()
    # Create a mock/dummy CompiledWorkflowPlan for the test
    mock_plan = CompiledWorkflowPlan(
        execution_id=uuid.uuid4(),
        workflow_name="test_workflow",
        concurrency=1,
        steps=[],
        context_snapshot={},
    )
    result = orchestrator.execute_plan(mock_plan)
    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.message == "Plan executed successfully."
