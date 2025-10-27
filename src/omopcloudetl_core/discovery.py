# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from .abstractions.secrets import BaseSecretsProvider, EnvironmentSecretsProvider
from .config.models import SecretsConfig


class DiscoveryManager:
    """
    Manages the discovery and instantiation of pluggable components.

    This is a placeholder implementation to allow other components to be tested.
    """

    def get_secrets_provider(self, config: SecretsConfig) -> BaseSecretsProvider:
        """
        Gets a secrets provider based on the provided configuration.
        """
        # This is a basic implementation that only supports the environment provider
        if config.provider_type == "env":
            return EnvironmentSecretsProvider()

        # In the future, this will use entry points to discover other providers
        from .exceptions import DiscoveryError
        raise DiscoveryError(f"Secrets provider type '{config.provider_type}' not found.")
