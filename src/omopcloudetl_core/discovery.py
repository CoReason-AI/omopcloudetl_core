# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from importlib.metadata import entry_points
from typing import Dict, Tuple, Type

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


class DiscoveryManager:
    """Manages the discovery and instantiation of pluggable components."""

    def __init__(self):
        self._secrets_providers: Dict[str, Type[BaseSecretsProvider]] = {}
        self._connections: Dict[str, Type[BaseConnection]] = {}
        self._orchestrators: Dict[str, Type[BaseOrchestrator]] = {}

    def _discover_components(self, entry_point_group: str) -> Dict[str, Type]:
        """Generic discovery method for a given entry point group."""
        discovered_components = {}
        try:
            # Use importlib.metadata to find installed plugins
            eps = entry_points()
            group_eps = []
            if hasattr(eps, 'select'):  # New API in Python 3.10+
                group_eps = eps.select(group=entry_point_group)
            elif isinstance(eps, dict):  # Deprecated dict-based API
                group_eps = eps.get(entry_point_group, [])
            else:  # Fallback for iterables, like the list from mocks
                group_eps = eps

            for entry in group_eps:
                discovered_components[entry.name] = entry.load()
        except Exception as e:
            raise DiscoveryError(f"Failed to discover or load components from group '{entry_point_group}'") from e
        return discovered_components

    def get_secrets_provider(self, config: SecretsConfig = None) -> BaseSecretsProvider:
        """Discovers and instantiates a secrets provider."""
        if not config:
            return EnvironmentSecretsProvider()

        if not self._secrets_providers:
            self._secrets_providers = self._discover_components("omopcloudetl.secrets")
            self._secrets_providers["environment"] = EnvironmentSecretsProvider

        provider_class = self._secrets_providers.get(config.provider_type)
        if not provider_class:
            raise DiscoveryError(f"Secrets provider '{config.provider_type}' not found.")

        try:
            return provider_class(**config.configuration)
        except Exception as e:
            raise DiscoveryError(f"Failed to instantiate secrets provider '{config.provider_type}'.") from e

    def get_connection(self, config: ConnectionConfig) -> BaseConnection:
        """Discovers and instantiates a database connection provider."""
        if not self._connections:
            self._connections = self._discover_components("omopcloudetl.providers")

        connection_class = self._connections.get(config.provider_type)
        if not connection_class:
            raise DiscoveryError(f"Connection provider '{config.provider_type}' not found.")

        try:
            # Pass the entire config model to the constructor
            return connection_class(config)
        except Exception as e:
            raise DiscoveryError(f"Failed to instantiate connection provider '{config.provider_type}'.") from e

    def get_orchestrator(self, config: OrchestratorConfig) -> BaseOrchestrator:
        """Discovers and instantiates a workflow orchestrator."""
        if not self._orchestrators:
            self._orchestrators = self._discover_components("omopcloudetl.orchestrators")

        orchestrator_class = self._orchestrators.get(config.type)
        if not orchestrator_class:
            raise DiscoveryError(f"Orchestrator '{config.type}' not found.")

        try:
            return orchestrator_class(**config.configuration)
        except Exception as e:
            raise DiscoveryError(f"Failed to instantiate orchestrator '{config.type}'.") from e

    def get_generators(self, connection: BaseConnection) -> Tuple[BaseSQLGenerator, BaseDDLGenerator]:
        """
        Retrieves the SQL and DDL generator classes from a connection instance
        and initializes them.
        """
        if not hasattr(connection, "SQL_GENERATOR_CLASS") or not hasattr(connection, "DDL_GENERATOR_CLASS"):
            raise DiscoveryError(
                f"Connection provider '{connection.provider_type}' does not define "
                "SQL_GENERATOR_CLASS and DDL_GENERATOR_CLASS attributes."
            )

        sql_generator = connection.SQL_GENERATOR_CLASS()
        ddl_generator = connection.DDL_GENERATOR_CLASS()
        return sql_generator, ddl_generator
