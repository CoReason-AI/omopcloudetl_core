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
from typing import Any, Dict, List, Union
from uuid import uuid4
import networkx as nx
import yaml
from pydantic import ValidationError

from omopcloudetl_core.abstractions.connections import BaseConnection
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.discovery import DiscoveryManager
from omopcloudetl_core.exceptions import CompilationError, DMLValidationError, WorkflowError
from omopcloudetl_core.models.dml import DMLDefinition
from omopcloudetl_core.models.workflow import (
    BulkLoadWorkflowStep,
    CompiledBulkLoadStep,
    CompiledSQLStep,
    CompiledWorkflowPlan,
    DDLWorkflowStep,
    DMLWorkflowStep,
    SQLWorkflowStep,
    WorkflowConfig,
)
from omopcloudetl_core.specifications.manager import SpecificationManager
from omopcloudetl_core.sql_tools import apply_query_tag, render_jinja_template, split_sql_script


class WorkflowCompiler:
    """
    Orchestrates the compilation of a workflow definition into an executable plan.

    This class is responsible for parsing the workflow configuration, validating
    the dependency graph (DAG) of its steps, and compiling each step into a
    structured, executable format. It does not perform any execution itself,
    only compilation.
    """

    def __init__(self, project_config: ProjectConfig, connection: BaseConnection):
        self.project_config = project_config
        self.connection = connection
        self.discovery_manager = DiscoveryManager()
        self.spec_manager = SpecificationManager()
        self.sql_generator, self.ddl_generator = self.discovery_manager.get_generators(connection)

    def _validate_dag(self, steps: List[Any]) -> None:
        """
        Validates the dependency graph of the workflow steps.

        Args:
            steps: A list of workflow step objects.

        Raises:
            WorkflowError: If the DAG is invalid (e.g., contains cycles).
        """
        graph = nx.DiGraph()
        step_names = {step.name for step in steps}

        for step in steps:
            graph.add_node(step.name)
            for dep in step.depends_on:
                if dep not in step_names:
                    raise WorkflowError(f"Step '{step.name}' has an undefined dependency: '{dep}'")
                graph.add_edge(dep, step.name)

        if not nx.is_directed_acyclic_graph(graph):
            raise WorkflowError("Workflow contains a circular dependency (cycle) in its steps.")

    def _build_context(self) -> Dict[str, Any]:
        """Builds the execution context from the project configuration."""
        return {"schemas": self.project_config.schemas}

    def compile(self, workflow_config: WorkflowConfig, workflow_base_path: Path) -> CompiledWorkflowPlan:
        """
        Compiles a workflow configuration into an executable plan.

        Args:
            workflow_config: The user-defined workflow configuration.
            workflow_base_path: The base path for resolving relative file paths
                                in the workflow steps.

        Returns:
            A `CompiledWorkflowPlan` object.
        """
        execution_id = uuid4()
        context = self._build_context()
        self._validate_dag(workflow_config.steps)

        compiled_steps: List[Union[CompiledSQLStep, CompiledBulkLoadStep]] = []

        for step in workflow_config.steps:
            query_tag_context = {
                "omopcloudetl_tool": "WorkflowCompiler",
                "execution_id": str(execution_id),
                "step_name": step.name,
                "workflow_name": workflow_config.workflow_name,
            }

            if isinstance(step, DMLWorkflowStep):
                dml_path = workflow_base_path / step.dml_file
                try:
                    with open(dml_path, "r") as f:
                        raw_dml = f.read()
                    rendered_dml = render_jinja_template(raw_dml, context)
                    dml_def = DMLDefinition.model_validate(yaml.safe_load(rendered_dml))
                except (IOError, ValidationError, yaml.YAMLError) as e:
                    raise DMLValidationError(f"Failed to load, render, or validate DML file {dml_path}") from e

                sql = self.sql_generator.generate_transform_sql(dml_def, context)
                tagged_sql = apply_query_tag(sql, query_tag_context)
                compiled_steps.append(
                    CompiledSQLStep(name=step.name, depends_on=step.depends_on, sql_statements=[tagged_sql])
                )

            elif isinstance(step, DDLWorkflowStep):
                spec = self.spec_manager.fetch_specification(step.cdm_version)
                schema_name = context["schemas"].get(step.target_schema_ref)
                if not schema_name:
                    raise CompilationError(f"Schema reference '{step.target_schema_ref}' not found in project config.")

                ddl_statements = self.ddl_generator.generate_ddl(spec, schema_name, step.options)
                tagged_ddl = [apply_query_tag(s, query_tag_context) for s in ddl_statements]
                compiled_steps.append(
                    CompiledSQLStep(name=step.name, depends_on=step.depends_on, sql_statements=tagged_ddl)
                )

            elif isinstance(step, SQLWorkflowStep):
                sql_path = workflow_base_path / step.sql_file
                try:
                    with open(sql_path, "r") as f:
                        raw_sql = f.read()
                except IOError as e:
                    raise CompilationError(f"Failed to read SQL file {sql_path}") from e

                rendered_sql = render_jinja_template(raw_sql, context)
                sql_statements = split_sql_script(rendered_sql)
                tagged_sql = [apply_query_tag(s, query_tag_context) for s in sql_statements]
                compiled_steps.append(
                    CompiledSQLStep(name=step.name, depends_on=step.depends_on, sql_statements=tagged_sql)
                )

            elif isinstance(step, BulkLoadWorkflowStep):
                schema_name = context["schemas"].get(step.target_schema_ref)
                if not schema_name:
                    raise CompilationError(f"Schema reference '{step.target_schema_ref}' not found in project config.")

                resolved_uri = render_jinja_template(step.source_uri_pattern, context)

                compiled_steps.append(
                    CompiledBulkLoadStep(
                        name=step.name,
                        depends_on=step.depends_on,
                        source_uri=resolved_uri,
                        target_schema=schema_name,
                        target_table=step.target_table,
                        source_format_options=step.options.get("source_format_options", {}),
                        load_options=step.options.get("load_options", {}),
                    )
                )

        return CompiledWorkflowPlan(
            execution_id=execution_id,
            workflow_name=workflow_config.workflow_name,
            concurrency=workflow_config.concurrency,
            steps=compiled_steps,
            context_snapshot=context,
        )
