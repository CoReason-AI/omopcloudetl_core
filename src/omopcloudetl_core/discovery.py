# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from importlib import metadata
from typing import Dict, Type, Tuple

from omopcloudetl_core.abstractions.connections import BaseConnection
from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator
from omopcloudetl_core.abstractions.orchestrators import BaseOrchestrator
from omopcloudetl_core.abstractions.secrets import (
    BaseSecretsProvider,
    EnvironmentSecretsProvider,
)
from omopcloudetl_core.config.models import (
    ConnectionConfig,
    OrchestratorConfig,
    SecretsConfig,
)
from omopcloudetl_core.exceptions import DiscoveryError
from omopcloudetl_core.logging import logger


class DiscoveryManager:
    """Discovers and instantiates pluggable components using entry points."""

    def _discover_components(self, entry_point_group: str) -> Dict[str, Type]:
        """
        Discovers all available components for a given entry point group.

        Args:
            entry_point_group: The name of the entry point group to scan.

        Returns:
            A dictionary mapping component names to their loaded classes.
        """
        components = {}
        try:
            entry_points = metadata.entry_points(group=entry_point_group)
            for entry_point in entry_points:
                components[entry_point.name] = entry_point.load()
        except Exception as e:
            raise DiscoveryError(f"Failed to discover components for group '{entry_point_group}': {e}") from e
        return components

    def get_secrets_provider(self, config: SecretsConfig | None) -> BaseSecretsProvider:
        """
        Gets an instance of a secrets provider based on the configuration.

        Defaults to EnvironmentSecretsProvider if no provider is specified.
        """
        if not config or not config.provider_type:
            logger.debug("No secrets provider configured, defaulting to EnvironmentSecretsProvider.")
            return EnvironmentSecretsProvider()

        providers = self._discover_components("omopcloudetl.secrets")
        provider_class = providers.get(config.provider_type)

        if not provider_class:
            raise DiscoveryError(f"Secrets provider '{config.provider_type}' not found.")

        return provider_class(**config.configuration)

    def get_connection(self, config: ConnectionConfig) -> BaseConnection:
        """Gets an instance of a database connection provider."""
        providers = self._discover_components("omopcloudetl.providers")
        provider_class = providers.get(config.provider_type)

        if not provider_class:
            raise DiscoveryError(f"Connection provider '{config.provider_type}' not found.")

        return provider_class(config)

    def get_orchestrator(self, config: OrchestratorConfig) -> BaseOrchestrator:
        """Gets an instance of an orchestrator."""
        orchestrators = self._discover_components("omopcloudetl.orchestrators")
        orchestrator_class = orchestrators.get(config.type)

        if not orchestrator_class:
            raise DiscoveryError(f"Orchestrator '{config.type}' not found.")

        return orchestrator_class(config.configuration)

    def get_generators(self, connection: BaseConnection) -> Tuple[BaseSQLGenerator, BaseDDLGenerator]:
        """
        Retrieves and initializes the SQL and DDL generators from a connection instance.
        """
        try:
            sql_generator = connection.SQL_GENERATOR_CLASS()
            ddl_generator = connection.DDL_GENERATOR_CLASS()
            return sql_generator, ddl_generator
        except AttributeError as e:
            raise DiscoveryError(
                f"Connection provider '{connection.provider_type}' does not correctly "
                "define 'SQL_GENERATOR_CLASS' or 'DDL_GENERATOR_CLASS'."
            ) from e
