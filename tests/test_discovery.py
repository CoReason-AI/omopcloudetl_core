# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from unittest.mock import MagicMock, patch

import pytest
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
from omopcloudetl_core.discovery import DiscoveryManager


# Mock provider classes for testing
class MockSecretsProvider(BaseSecretsProvider):
    def get_secret(self, secret_identifier: str) -> str:
        return "mock"


class MockSQLGenerator(BaseSQLGenerator):
    def generate_transform_sql(self, dml_definition, context) -> str:
        return "SELECT 1;"


class MockDDLGenerator(BaseDDLGenerator):
    def generate_ddl(self, specification, schema_name, options) -> list[str]:
        return ["CREATE TABLE..."]


class MockConnection(BaseConnection):
    SQL_GENERATOR_CLASS = MockSQLGenerator
    DDL_GENERATOR_CLASS = MockDDLGenerator

    def __init__(self, config):
        self._config = config

    @property
    def provider_type(self) -> str:
        return "mock_conn"

    @property
    def scalability_tier(self):
        return None

    def connect(self):
        pass

    def close(self):
        pass

    def execute_sql(self, sql, commit=True):
        pass

    def bulk_load(self, source_uri, target_schema, target_table, source_format_options, load_options):
        pass


class MockOrchestrator(BaseOrchestrator):
    def execute_plan(self, plan, dry_run=False, resume=False):
        return {"success": True}


@pytest.fixture
def discovery_manager():
    """Fixture to provide a DiscoveryManager instance."""
    return DiscoveryManager()


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_connection_discovered(mock_entry_points, discovery_manager):
    """Test discovery and instantiation of a connection provider."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_conn"
    mock_entry.load.return_value = MockConnection

    # Mock the new call pattern: entry_points().select(...)
    mock_eps_object = MagicMock()
    mock_eps_object.select.return_value = [mock_entry]
    mock_entry_points.return_value = mock_eps_object

    config = ConnectionConfig(provider_type="mock_conn")
    conn = discovery_manager.get_connection(config)

    assert isinstance(conn, MockConnection)
    mock_entry_points.assert_called_once()
    mock_eps_object.select.assert_called_with(group="omopcloudetl.providers")


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_orchestrator_discovered(mock_entry_points, discovery_manager):
    """Test discovery and instantiation of an orchestrator."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_orch"
    mock_entry.load.return_value = MockOrchestrator

    mock_eps_object = MagicMock()
    mock_eps_object.select.return_value = [mock_entry]
    mock_entry_points.return_value = mock_eps_object

    config = OrchestratorConfig(type="mock_orch")
    orch = discovery_manager.get_orchestrator(config)

    assert isinstance(orch, MockOrchestrator)
    mock_entry_points.assert_called_once()
    mock_eps_object.select.assert_called_with(group="omopcloudetl.orchestrators")


def test_get_generators(discovery_manager):
    """Test retrieval of generator instances from a connection object."""
    mock_connection = MockConnection(ConnectionConfig(provider_type="mock_conn"))
    sql_gen, ddl_gen = discovery_manager.get_generators(mock_connection)

    assert isinstance(sql_gen, MockSQLGenerator)
    assert isinstance(ddl_gen, MockDDLGenerator)


def test_get_secrets_provider_default(discovery_manager):
    """Test that the default EnvironmentSecretsProvider is returned."""
    provider = discovery_manager.get_secrets_provider(config=None)
    assert isinstance(provider, EnvironmentSecretsProvider)


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_secrets_provider_discovered(mock_entry_points, discovery_manager):
    """Test discovery of a secrets provider."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_secrets"
    mock_entry.load.return_value = MockSecretsProvider

    mock_eps_object = MagicMock()
    mock_eps_object.select.return_value = [mock_entry]
    mock_entry_points.return_value = mock_eps_object

    config = SecretsConfig(provider_type="mock_secrets")
    provider = discovery_manager.get_secrets_provider(config)

    assert isinstance(provider, MockSecretsProvider)
    mock_entry_points.assert_called_once()
    mock_eps_object.select.assert_called_with(group="omopcloudetl.secrets")
