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
from unittest.mock import MagicMock, patch
from omopcloudetl_core.discovery import DiscoveryManager
from omopcloudetl_core.exceptions import DiscoveryError
from omopcloudetl_core.abstractions.connections import BaseConnection
from omopcloudetl_core.abstractions.orchestrators import BaseOrchestrator
from omopcloudetl_core.abstractions.secrets import BaseSecretsProvider, EnvironmentSecretsProvider
from omopcloudetl_core.config.models import ConnectionConfig, OrchestratorConfig, SecretsConfig


class MockConnection(BaseConnection):
    SQL_GENERATOR_CLASS = MagicMock()
    DDL_GENERATOR_CLASS = MagicMock()
    provider_type = "mock_conn"

    def __init__(self, config):
        super().__init__(config)

    def scalability_tier(self):
        return "TIER_1"

    def connect(self):
        pass

    def close(self):
        pass

    def execute_sql(self, sql, params=None, commit=True):
        pass

    def bulk_load(self, source_uri, target_schema, target_table, source_format_options, load_options):
        pass


class MockOrchestrator(BaseOrchestrator):
    def execute_plan(self, plan, dry_run=False, resume=False):
        pass


class MockSecretsProvider(BaseSecretsProvider):
    def get_secret(self, secret_identifier: str) -> str:
        return "secret"


@pytest.fixture
def discovery_manager():
    """Provides a DiscoveryManager instance."""
    return DiscoveryManager()


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_connection_success(mock_entry_points, discovery_manager):
    """Tests successfully discovering and instantiating a connection provider."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_conn"
    mock_entry.load.return_value = MockConnection
    mock_entry_points.return_value.select.return_value = [mock_entry]

    config = ConnectionConfig(provider_type="mock_conn")
    connection = discovery_manager.get_connection(config)
    assert isinstance(connection, MockConnection)


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_orchestrator_success(mock_entry_points, discovery_manager):
    """Tests successfully discovering and instantiating an orchestrator."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_orch"
    mock_entry.load.return_value = MockOrchestrator
    mock_entry_points.return_value.select.return_value = [mock_entry]

    config = OrchestratorConfig(type="mock_orch")
    orchestrator = discovery_manager.get_orchestrator(config)
    assert isinstance(orchestrator, MockOrchestrator)


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_secrets_provider_success(mock_entry_points, discovery_manager):
    """Tests successfully discovering and instantiating a secrets provider."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_secrets"
    mock_entry.load.return_value = MockSecretsProvider
    mock_entry_points.return_value.select.return_value = [mock_entry]

    config = SecretsConfig(provider_type="mock_secrets")
    provider = discovery_manager.get_secrets_provider(config)
    assert isinstance(provider, MockSecretsProvider)


def test_get_default_secrets_provider(discovery_manager):
    """Tests that the default environment secrets provider is returned when no config is provided."""
    provider = discovery_manager.get_secrets_provider(None)
    assert isinstance(provider, EnvironmentSecretsProvider)


@patch("omopcloudetl_core.discovery.entry_points")
def test_component_not_found(mock_entry_points, discovery_manager):
    """Tests that a DiscoveryError is raised when a component is not found."""
    mock_entry_points.return_value.select.return_value = []
    with pytest.raises(DiscoveryError, match="not found"):
        discovery_manager.get_connection(ConnectionConfig(provider_type="non_existent"))
    with pytest.raises(DiscoveryError, match="not found"):
        discovery_manager.get_orchestrator(OrchestratorConfig(type="non_existent"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_component_instantiation_fails(mock_entry_points, discovery_manager):
    """Tests that a DiscoveryError is raised if component instantiation fails."""
    BadConnection = MagicMock(side_effect=Exception("Init failed"))
    mock_entry = MagicMock()
    mock_entry.name = "bad_conn"
    mock_entry.load.return_value = BadConnection
    mock_entry_points.return_value.select.return_value = [mock_entry]

    with pytest.raises(DiscoveryError, match="Failed to instantiate"):
        discovery_manager.get_connection(ConnectionConfig(provider_type="bad_conn"))
