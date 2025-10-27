# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from importlib.abc import Traversable
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

import networkx as nx
import yaml
from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.exceptions import CompilationError, WorkflowError
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
from omopcloudetl_core.sql_tools import apply_query_tag, render_jinja_template


class WorkflowCompiler:
    """Compiles a workflow configuration into an executable plan."""

    def __init__(
        self,
        project_config: ProjectConfig,
        sql_generator: BaseSQLGenerator,
        ddl_generator: BaseDDLGenerator,
        spec_manager: SpecificationManager,
    ):
        self.project_config = project_config
        self.sql_generator = sql_generator
        self.ddl_generator = ddl_generator
        self.spec_manager = spec_manager

    def _validate_dag(self, workflow_config: WorkflowConfig):
        """Validates the dependency graph of the workflow."""
        graph = nx.DiGraph()
        step_names = {step.name for step in workflow_config.steps}
        for step in workflow_config.steps:
            graph.add_node(step.name)
            for dep in step.depends_on:
                if dep not in step_names:
                    raise WorkflowError(f"Step '{step.name}' has an undefined dependency: '{dep}'")
                graph.add_edge(dep, step.name)

        if not nx.is_directed_acyclic_graph(graph):
            cycles = list(nx.simple_cycles(graph))
            raise WorkflowError(f"Workflow contains cycles: {cycles}")

    def compile(
        self, workflow_config: WorkflowConfig, workflow_base_path: Traversable
    ) -> CompiledWorkflowPlan:
        """
        Compiles the workflow configuration into an executable plan.

        Args:
            workflow_config: The user-defined workflow configuration.
            workflow_base_path: A traversable path object pointing to the
                                root directory of the workflow definition files.

        Returns:
            A CompiledWorkflowPlan object.
        """
        self._validate_dag(workflow_config)
        execution_id = uuid4()
        context = {"schemas": self.project_config.schemas}
        compiled_steps = []

        for step in workflow_config.steps:
            query_context = {
                "execution_id": str(execution_id),
                "workflow": workflow_config.workflow_name,
                "step": step.name,
            }

            if isinstance(step, DMLWorkflowStep):
                try:
                    dml_file_path = workflow_base_path.joinpath(step.dml_file)
                    dml_content = dml_file_path.read_text()
                    dml_data = yaml.safe_load(render_jinja_template(dml_content, context))
                    dml_def = DMLDefinition(**dml_data)
                    sql = self.sql_generator.generate_transform_sql(dml_def, context)
                    tagged_sql = apply_query_tag(sql, query_context)
                    compiled_steps.append(
                        CompiledSQLStep(
                            name=step.name,
                            depends_on=step.depends_on,
                            sql_statements=[tagged_sql],
                        )
                    )
                except Exception as e:
                    raise CompilationError(f"Failed to compile DML step '{step.name}'") from e

            elif isinstance(step, DDLWorkflowStep):
                schema = self.project_config.schemas.get(step.target_schema_ref)
                if not schema:
                    raise CompilationError(f"Schema reference '{step.target_schema_ref}' not found in project config.")

                spec = self.spec_manager.fetch_specification(step.cdm_version)
                ddl_statements = self.ddl_generator.generate_ddl(spec, schema, step.options)
                tagged_statements = [apply_query_tag(s, query_context) for s in ddl_statements]
                compiled_steps.append(
                    CompiledSQLStep(
                        name=step.name,
                        depends_on=step.depends_on,
                        sql_statements=tagged_statements,
                    )
                )

            elif isinstance(step, SQLWorkflowStep):
                sql_file_path = workflow_base_path.joinpath(step.sql_file)
                sql_content = sql_file_path.read_text()
                rendered_sql = render_jinja_template(sql_content, context)
                tagged_sql = apply_query_tag(rendered_sql, query_context)
                compiled_steps.append(
                    CompiledSQLStep(
                        name=step.name,
                        depends_on=step.depends_on,
                        sql_statements=[tagged_sql],
                    )
                )

            elif isinstance(step, BulkLoadWorkflowStep):
                resolved_uri = render_jinja_template(step.source_uri_pattern, context)
                target_schema = self.project_config.schemas.get(step.target_schema_ref)
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

        return CompiledWorkflowPlan(
            execution_id=execution_id,
            workflow_name=workflow_config.workflow_name,
            concurrency=workflow_config.concurrency,
            steps=compiled_steps,
            context_snapshot=context,
        )
