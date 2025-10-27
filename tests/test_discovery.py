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
from omopcloudetl_core.exceptions import DiscoveryError


# Mock provider classes for testing
class MockSecretsProvider(BaseSecretsProvider):
    def __init__(self, **kwargs):
        pass

    def get_secret(self, secret_identifier: str) -> str:
        return "mock"


class MockSQLGenerator(BaseSQLGenerator):
    def generate_transform_sql(self, dml_definition, context) -> str:  # pragma: no cover
        return "SELECT 1;"


class MockDDLGenerator(BaseDDLGenerator):
    def generate_ddl(self, specification, schema_name, options) -> list[str]:  # pragma: no cover
        return ["CREATE TABLE..."]


class MockConnection(BaseConnection):
    SQL_GENERATOR_CLASS = MockSQLGenerator
    DDL_GENERATOR_CLASS = MockDDLGenerator

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
    def __init__(self, **kwargs):
        pass

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
    mock_eps_object.select.assert_called_with(group="omopcloudetl.secrets")


@patch("omopcloudetl_core.discovery.entry_points")
def test_discovery_with_deprecated_api(mock_entry_points, discovery_manager):
    """Test discovery using the pre-Python 3.10 dictionary-based API."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_conn"
    mock_entry.load.return_value = MockConnection

    # Simulate the old dict-based return value
    mock_eps_dict = {"omopcloudetl.providers": [mock_entry]}
    mock_entry_points.return_value = mock_eps_dict

    config = ConnectionConfig(provider_type="mock_conn")
    conn = discovery_manager.get_connection(config)
    assert isinstance(conn, MockConnection)


@patch("omopcloudetl_core.discovery.entry_points")
def test_discovery_with_iterable_fallback(mock_entry_points, discovery_manager):
    """Test discovery using the iterable fallback for mocks."""
    mock_entry = MagicMock()
    mock_entry.name = "mock_conn"
    mock_entry.load.return_value = MockConnection

    # Simulate a list-based return value, as a mock might provide
    mock_entry_points.return_value = [mock_entry]

    # This will fail as is, because the list doesn't have a 'name' to match the group
    # So we need to call a private method to test this branch effectively
    components = discovery_manager._discover_components("omopcloudetl.providers")
    assert "mock_conn" in components


@patch("omopcloudetl_core.discovery.entry_points")
def test_discover_components_load_error(mock_entry_points, discovery_manager):
    """Test DiscoveryError when an entry point fails to load."""
    mock_entry = MagicMock()
    mock_entry.name = "failing_loader"
    mock_entry.load.side_effect = ImportError("Module not found")
    mock_entry_points.return_value = {"omopcloudetl.providers": [mock_entry]}

    with pytest.raises(DiscoveryError, match="Failed to load component 'failing_loader'"):
        discovery_manager.get_connection(ConnectionConfig(provider_type="failing_loader"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_connection_not_found(mock_entry_points, discovery_manager):
    """Test DiscoveryError when a connection provider is not found."""
    mock_entry_points.return_value = {"omopcloudetl.providers": []}
    with pytest.raises(DiscoveryError, match="Connection provider 'non_existent' not found"):
        discovery_manager.get_connection(ConnectionConfig(provider_type="non_existent"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_connection_instantiation_error(mock_entry_points, discovery_manager):
    """Test DiscoveryError when a connection provider fails to instantiate."""
    mock_entry = MagicMock()
    mock_entry.name = "failing_conn"
    # The mock connection expects a config object, so instantiating without one will fail
    mock_entry.load.return_value = MagicMock(side_effect=TypeError("Bad arguments"))
    mock_entry_points.return_value = {"omopcloudetl.providers": [mock_entry]}

    with pytest.raises(DiscoveryError, match="Failed to instantiate connection provider"):
        discovery_manager.get_connection(ConnectionConfig(provider_type="failing_conn"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_orchestrator_not_found(mock_entry_points, discovery_manager):
    """Test DiscoveryError when an orchestrator is not found."""
    mock_entry_points.return_value = {"omopcloudetl.orchestrators": []}
    with pytest.raises(DiscoveryError, match="Orchestrator 'non_existent' not found"):
        discovery_manager.get_orchestrator(OrchestratorConfig(type="non_existent"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_orchestrator_instantiation_error(mock_entry_points, discovery_manager):
    """Test DiscoveryError when an orchestrator fails to instantiate."""
    mock_entry = MagicMock()
    mock_entry.name = "failing_orch"
    mock_entry.load.return_value = MagicMock(side_effect=TypeError("Bad arguments"))
    mock_entry_points.return_value = {"omopcloudetl.orchestrators": [mock_entry]}

    with pytest.raises(DiscoveryError, match="Failed to instantiate orchestrator"):
        discovery_manager.get_orchestrator(OrchestratorConfig(type="failing_orch"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_secrets_provider_not_found(mock_entry_points, discovery_manager):
    """Test DiscoveryError when a secrets provider is not found."""
    mock_entry_points.return_value = {"omopcloudetl.secrets": []}
    with pytest.raises(DiscoveryError, match="Secrets provider 'non_existent' not found"):
        discovery_manager.get_secrets_provider(SecretsConfig(provider_type="non_existent"))


@patch("omopcloudetl_core.discovery.entry_points")
def test_get_secrets_provider_instantiation_error(mock_entry_points, discovery_manager):
    """Test DiscoveryError when a secrets provider fails to instantiate."""
    mock_entry = MagicMock()
    mock_entry.name = "failing_secrets"
    mock_entry.load.return_value = MagicMock(side_effect=TypeError("Bad arguments"))
    mock_entry_points.return_value = {"omopcloudetl.secrets": [mock_entry]}

    with pytest.raises(DiscoveryError, match="Failed to instantiate secrets provider"):
        discovery_manager.get_secrets_provider(SecretsConfig(provider_type="failing_secrets"))


def test_get_generators_missing_attribute(discovery_manager):
    """Test DiscoveryError when a connection is missing generator class attributes."""

    class NoGenConnection(BaseConnection):
        @property
        def provider_type(self) -> str: return "no_gen"
        @property
        def scalability_tier(self): return None
        def connect(self): pass
        def close(self): pass
        def execute_sql(self, sql, commit=True): pass
        def bulk_load(self, source_uri, target_schema, target_table, source_format_options, load_options): pass

    no_gen_conn = NoGenConnection(ConnectionConfig(provider_type="no_gen"))
    with pytest.raises(DiscoveryError, match="does not define SQL_GENERATOR_CLASS"):
        discovery_manager.get_generators(no_gen_conn)
