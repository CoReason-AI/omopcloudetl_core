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
from typing import Optional

from omopcloudetl_core.abstractions.secrets import BaseSecretsProvider, EnvironmentSecretsProvider
from omopcloudetl_core.config.models import SecretsConfig
from omopcloudetl_core.exceptions import DiscoveryError


class DiscoveryManager:
    """Discovers and instantiates pluggable components."""

    def get_secrets_provider(self, config: Optional[SecretsConfig]) -> BaseSecretsProvider:
        """
        Gets a secrets provider based on the provided configuration.
        Defaults to EnvironmentSecretsProvider if no config is provided.
        """
        if not config or config.provider_type == "environment":
            return EnvironmentSecretsProvider()

        # Placeholder for entry point discovery
        discovered_providers = entry_points(group="omopcloudetl.secrets")
        for entry_point in discovered_providers:
            if entry_point.name == config.provider_type:
                provider_class = entry_point.load()
                return provider_class(**config.configuration)

        raise DiscoveryError(f"Discovery failed: Secrets provider '{config.provider_type}' not found.")

    def get_connection(self, config):  # pragma: no cover
        """Gets a database connection provider."""
        raise NotImplementedError

    def get_orchestrator(self, config):  # pragma: no cover
        """Gets an orchestrator."""
        raise NotImplementedError

    def get_generators(self, connection):  # pragma: no cover
        """Gets DDL and SQL generators for a connection."""
        raise NotImplementedError
