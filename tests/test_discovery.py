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
from unittest.mock import patch

from omopcloudetl_core.discovery import DiscoveryManager
from omopcloudetl_core.abstractions.secrets import BaseSecretsProvider, EnvironmentSecretsProvider
from omopcloudetl_core.exceptions import DiscoveryError
from omopcloudetl_core.config.models import SecretsConfig


# A mock provider for testing entry point loading
class MockSecretsProvider(BaseSecretsProvider):
    def __init__(self, **kwargs):
        self.config = kwargs

    def get_secret(self, secret_identifier: str) -> str:
        return "mock-secret"


class TestDiscoveryManager:
    @pytest.fixture
    def manager(self):
        return DiscoveryManager()

    def test_get_secrets_provider_default(self, manager):
        """Test that the default secrets provider is EnvironmentSecretsProvider."""
        provider = manager.get_secrets_provider(None)
        assert isinstance(provider, EnvironmentSecretsProvider)

    def test_get_secrets_provider_environment(self, manager):
        """Test that requesting 'environment' provider returns the correct class."""
        config = SecretsConfig(provider_type="environment")
        provider = manager.get_secrets_provider(config)
        assert isinstance(provider, EnvironmentSecretsProvider)

    def test_get_secrets_provider_unsupported_raises_error(self, manager):
        """Test that an unsupported provider type raises a DiscoveryError."""
        config = SecretsConfig(provider_type="unsupported_provider")
        with patch("omopcloudetl_core.discovery.entry_points", return_value=[]):
            with pytest.raises(DiscoveryError, match="Discovery failed"):
                manager.get_secrets_provider(config)

    def test_get_secrets_provider_entry_point_loading(self, manager):
        """Simulate discovering a provider via entry points."""

        class MockEntryPoint:
            name = "mock_provider"

            def load(self):
                return MockSecretsProvider

        mock_ep = MockEntryPoint()

        config = SecretsConfig(provider_type="mock_provider", configuration={"key": "value"})
        with patch("omopcloudetl_core.discovery.entry_points", return_value=[mock_ep]) as mock_call:
            provider = manager.get_secrets_provider(config)
            assert isinstance(provider, MockSecretsProvider)
            assert provider.config == {"key": "value"}
            mock_call.assert_called_once_with(group="omopcloudetl.secrets")

    def test_get_connection_not_implemented(self, manager):
        """Test that get_connection raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            manager.get_connection("some_connection")

    def test_get_orchestrator_not_implemented(self, manager):
        """Test that get_orchestrator raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            manager.get_orchestrator("some_orchestrator")

    def test_get_generators_not_implemented(self, manager):
        """Test that get_generators raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            manager.get_generators("some_connection")
