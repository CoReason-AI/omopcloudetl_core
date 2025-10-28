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
from omopcloudetl_core.abstractions.secrets import EnvironmentSecretsProvider

@pytest.fixture
def discovery_manager():
    """Fixture for a DiscoveryManager."""
    return DiscoveryManager()

def test_get_secrets_provider_default(discovery_manager):
    """Test that the EnvironmentSecretsProvider is returned by default."""
    provider = discovery_manager.get_secrets_provider(None)
    assert isinstance(provider, EnvironmentSecretsProvider)

@patch("importlib.metadata.entry_points")
def test_get_secrets_provider_discovered(mock_entry_points, discovery_manager):
    """Test discovery of a secrets provider from an entry point."""
    mock_provider_class = MagicMock()
    mock_entry_point = MagicMock()
    mock_entry_point.name = "mock_provider"
    mock_entry_point.load.return_value = mock_provider_class
    mock_entry_points.return_value = [mock_entry_point]

    config = MagicMock()
    config.provider_type = "mock_provider"
    config.configuration = {"key": "value"}

    provider = discovery_manager.get_secrets_provider(config)
    mock_provider_class.assert_called_once_with(key="value")
    assert provider == mock_provider_class.return_value

def test_get_secrets_provider_not_found(discovery_manager):
    """Test that a DiscoveryError is raised when a provider is not found."""
    config = MagicMock()
    config.provider_type = "non_existent_provider"
    with pytest.raises(DiscoveryError):
        discovery_manager.get_secrets_provider(config)

@patch("importlib.metadata.entry_points")
def test_get_connection_success(mock_entry_points, discovery_manager):
    """Test successful discovery of a connection provider."""
    mock_conn_class = MagicMock()
    mock_entry_point = MagicMock()
    mock_entry_point.name = "mock_db"
    mock_entry_point.load.return_value = mock_conn_class
    mock_entry_points.return_value = [mock_entry_point]

    config = MagicMock()
    config.provider_type = "mock_db"

    connection = discovery_manager.get_connection(config)
    mock_conn_class.assert_called_once_with(config)
    assert connection == mock_conn_class.return_value

def test_get_connection_failure(discovery_manager):
    """Test that a DiscoveryError is raised when a connection provider is not found."""
    config = MagicMock()
    config.provider_type = "non_existent_db"
    with pytest.raises(DiscoveryError):
        discovery_manager.get_connection(config)

def test_get_generators_success(discovery_manager):
    """Test successful retrieval of generators from a connection instance."""
    mock_sql_gen = MagicMock()
    mock_ddl_gen = MagicMock()

    mock_connection = MagicMock()
    mock_connection.SQL_GENERATOR_CLASS = mock_sql_gen
    mock_connection.DDL_GENERATOR_CLASS = mock_ddl_gen

    sql_gen, ddl_gen = discovery_manager.get_generators(mock_connection)

    assert sql_gen == mock_sql_gen.return_value
    assert ddl_gen == mock_ddl_gen.return_value

def test_get_generators_failure(discovery_manager):
    """Test that a DiscoveryError is raised if generators are not defined on the connection."""
    mock_connection = MagicMock()
    del mock_connection.SQL_GENERATOR_CLASS

    with pytest.raises(DiscoveryError):
        discovery_manager.get_generators(mock_connection)
