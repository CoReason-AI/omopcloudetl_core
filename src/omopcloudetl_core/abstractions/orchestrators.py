# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from abc import ABC, abstractmethod
from typing import Any, Dict

from pydantic import BaseModel

from omopcloudetl_core.models.workflow import CompiledWorkflowPlan


class ExecutionResult(BaseModel):
    """Represents the outcome of a workflow execution."""

    success: bool
    message: str
    details: Dict[str, Any] = {}


class BaseOrchestrator(ABC):
    """Abstract base class for all workflow orchestrators."""

    @abstractmethod
    def execute_plan(self, plan: CompiledWorkflowPlan, dry_run: bool = False, resume: bool = False) -> ExecutionResult:  # pragma: no cover
        """
        Executes a compiled workflow plan.

        Args:
            plan: The compiled workflow plan to execute.
            dry_run: If True, the orchestrator should only print what it would do
                     without actually executing any SQL or data operations.
            resume: If True, the orchestrator should attempt to resume a previously
                    failed execution, skipping completed steps.

        Returns:
            A dictionary summarizing the execution result.
        """
        pass
