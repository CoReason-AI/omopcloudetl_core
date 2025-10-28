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
from typing import Any, Dict, List
from uuid import uuid4

import networkx as nx
import yaml
from pydantic import ValidationError

from omopcloudetl_core.abstractions.connections import BaseConnection
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.discovery import DiscoveryManager
from omopcloudetl_core.exceptions import CompilationError, DMLValidationError, WorkflowError
from omopcloudetl_core.logging import logger
from omopcloudetl_core.models.dml import DMLDefinition
from omopcloudetl_core.models.workflow import (
    CompiledBulkLoadStep,
    CompiledSQLStep,
    CompiledStep,
    CompiledWorkflowPlan,
    DMLWorkflowStep,
    DDLWorkflowStep,
    SQLWorkflowStep,
    BulkLoadWorkflowStep,
    WorkflowConfig,
    WorkflowStep,
)
from omopcloudetl_core.specifications.manager import SpecificationManager
from omopcloudetl_core.sql_tools import apply_query_tag, render_jinja_template, split_sql_script


class WorkflowCompiler:
    """
    Coordinates the compilation of a workflow configuration into an executable plan.
    This class contains no execution logic.
    """

    def __init__(self, project_config: ProjectConfig, connection: BaseConnection):
        self.project_config = project_config
        self.connection = connection
        self.discovery = DiscoveryManager()
        self.spec_manager = SpecificationManager()
        self.sql_generator, self.ddl_generator = self.discovery.get_generators(connection)

    def _validate_dag(self, steps: List[WorkflowStep]) -> None:
        """Validates the workflow's dependency graph for cycles."""
        graph = nx.DiGraph()
        step_names = {step.name for step in steps}
        for step in steps:
            graph.add_node(step.name)
            for dep in step.depends_on:
                if dep not in step_names:
                    raise WorkflowError(f"Step '{step.name}' has an undefined dependency: '{dep}'")
                graph.add_edge(dep, step.name)

        if not nx.is_directed_acyclic_graph(graph):
            cycles = list(nx.simple_cycles(graph))
            raise WorkflowError(f"Workflow contains cycles: {cycles}")

        logger.info("Workflow DAG is valid.")

    def _build_context(self, execution_id: str) -> Dict[str, Any]:
        """Builds the execution context for Jinja2 rendering and query tagging."""
        context = {
            "schemas": self.project_config.schemas,
            "execution_id": execution_id,
        }
        # Add any other relevant project-level configurations here
        return context

    def _read_and_render_file(self, file_path: Path, context: Dict[str, Any]) -> str:
        """Reads a file and renders it as a Jinja2 template."""
        if not file_path.is_file():
            raise CompilationError(f"File not found: {file_path}")
        try:
            template_str = file_path.read_text()
            return render_jinja_template(template_str, context)
        except Exception as e:
            raise CompilationError(f"Failed to read or render file {file_path}: {e}") from e

    def compile(self, workflow_config: WorkflowConfig, workflow_base_path: Path) -> CompiledWorkflowPlan:
        """
        Compiles a workflow configuration into a structured, executable plan.

        Args:
            workflow_config: The user-defined workflow configuration.
            workflow_base_path: The base path for resolving relative file paths in the workflow.

        Returns:
            A CompiledWorkflowPlan object.
        """
        logger.info(f"Starting compilation for workflow: '{workflow_config.workflow_name}'")
        self._validate_dag(workflow_config.steps)

        execution_id = uuid4()
        context = self._build_context(str(execution_id))

        compiled_steps: List[CompiledStep] = []
        for step in workflow_config.steps:
            query_context = {"execution_id": str(execution_id), "step_name": step.name}

            if isinstance(step, DMLWorkflowStep):
                dml_content = self._read_and_render_file(workflow_base_path / step.dml_file, context)
                try:
                    dml_def = DMLDefinition.model_validate(yaml.safe_load(dml_content))
                except (yaml.YAMLError, ValidationError) as e:
                    raise DMLValidationError(f"Invalid DML definition in '{step.dml_file}': {e}") from e

                sql = self.sql_generator.generate_transform_sql(dml_def, context)
                tagged_sql = apply_query_tag(sql, query_context)
                compiled_steps.append(
                    CompiledSQLStep(name=step.name, depends_on=step.depends_on, sql_statements=[tagged_sql])
                )

            elif isinstance(step, DDLWorkflowStep):
                spec = self.spec_manager.fetch_specification(step.cdm_version)
                schema = context["schemas"].get(step.target_schema_ref)
                if not schema:
                    raise CompilationError(f"Schema reference '{step.target_schema_ref}' not found in project config.")

                ddl_statements = self.ddl_generator.generate_ddl(spec, schema, step.options)
                tagged_statements = [apply_query_tag(s, query_context) for s in ddl_statements]
                compiled_steps.append(
                    CompiledSQLStep(name=step.name, depends_on=step.depends_on, sql_statements=tagged_statements)
                )

            elif isinstance(step, SQLWorkflowStep):
                sql_script = self._read_and_render_file(workflow_base_path / step.sql_file, context)
                statements = split_sql_script(sql_script)
                tagged_statements = [apply_query_tag(s, query_context) for s in statements]
                compiled_steps.append(
                    CompiledSQLStep(name=step.name, depends_on=step.depends_on, sql_statements=tagged_statements)
                )

            elif isinstance(step, BulkLoadWorkflowStep):
                resolved_uri = render_jinja_template(step.source_uri_pattern, context)
                target_schema = context["schemas"].get(step.target_schema_ref)
                if not target_schema:
                    raise CompilationError(f"Schema reference '{step.target_schema_ref}' not found in project config.")

                compiled_steps.append(
                    CompiledBulkLoadStep(
                        name=step.name,
                        depends_on=step.depends_on,
                        source_uri=resolved_uri,
                        target_schema=target_schema,
                        target_table=step.target_table,
                        source_format_options=step.options.get("source_format", {}),
                        load_options=step.options.get("load", {}),
                    )
                )

        logger.info("Workflow compilation completed successfully.")
        return CompiledWorkflowPlan(
            execution_id=execution_id,
            workflow_name=workflow_config.workflow_name,
            concurrency=workflow_config.concurrency,
            steps=compiled_steps,
            context_snapshot=context,
        )
