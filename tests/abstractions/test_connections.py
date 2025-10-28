# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Any, Dict, Optional, Sequence, Type

import pytest
from omopcloudetl_core.abstractions.connections import BaseConnection, ScalabilityTier
from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator
from omopcloudetl_core.config.models import ConnectionConfig
from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics


class MockSQLGenerator(BaseSQLGenerator):
    def generate_transform_sql(self, dml_definition: Any, context: Dict[str, Any]) -> str:
        return "SELECT 1;"


class MockDDLGenerator(BaseDDLGenerator):
    def generate_ddl(self, specification: Any, schema_name: str, options: Dict[str, Any]) -> list[str]:
        return ["CREATE TABLE mock;"]


class ConcreteConnection(BaseConnection):
    """A concrete implementation of BaseConnection for testing purposes."""

    SQL_GENERATOR_CLASS: Type[BaseSQLGenerator] = MockSQLGenerator
    DDL_GENERATOR_CLASS: Type[BaseDDLGenerator] = MockDDLGenerator

    @property
    def provider_type(self) -> str:
        return "mock_db"

    @property
    def scalability_tier(self) -> ScalabilityTier:
        return ScalabilityTier.TIER_3_SINGLE_NODE

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def execute_sql(self, sql: str, params: Optional[Sequence[Any]] = None, commit: bool = True) -> ExecutionMetrics:
        return ExecutionMetrics(rows_affected=1)

    def bulk_load(
        self,
        source_uri: str,
        target_schema: str,
        target_table: str,
        source_format_options: Dict[str, Any],
        load_options: Dict[str, Any],
    ) -> LoadMetrics:
        return LoadMetrics(rows_inserted=100, rows_rejected=0)

    def fetch_data(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        return []

    def table_exists(self, table_name: str, schema_name: str) -> bool:
        return True

    def post_load_maintenance(self, table_name: str, schema_name: str) -> None:
        pass

    def bulk_unload(
        self,
        target_uri: str,
        target_format: str,
        sql: str,
        unload_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        pass


@pytest.fixture
def connection_config():
    return ConnectionConfig(provider_type="mock_db", host="localhost")


@pytest.fixture
def concrete_connection(connection_config):
    return ConcreteConnection(connection_config)


def test_connection_initialization(concrete_connection, connection_config):
    assert concrete_connection.config == connection_config
    assert concrete_connection.provider_type == "mock_db"
    assert concrete_connection.scalability_tier == ScalabilityTier.TIER_3_SINGLE_NODE


def test_connection_execute_sql(concrete_connection):
    metrics = concrete_connection.execute_sql("SELECT * FROM table")
    assert isinstance(metrics, ExecutionMetrics)
    assert metrics.rows_affected == 1


def test_connection_bulk_load(concrete_connection):
    metrics = concrete_connection.bulk_load("uri", "schema", "table", {}, {})
    assert isinstance(metrics, LoadMetrics)
    assert metrics.rows_inserted == 100
