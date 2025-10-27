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
from typing import Any, Dict, Type
from omopcloudetl_core.abstractions.connections import BaseConnection, ScalabilityTier
from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator
from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics


class _TestSQLGenerator(BaseSQLGenerator):
    def generate_transform_sql(self, dml_definition: Any, context: Dict[str, Any]) -> str:
        return "SELECT 1;"


class _TestDDLGenerator(BaseDDLGenerator):
    def generate_ddl(self, specification: Any, schema_name: str, options: dict) -> list[str]:
        return ["CREATE TABLE test;"]


class ConcreteConnection(BaseConnection):
    SQL_GENERATOR_CLASS: Type[BaseSQLGenerator] = _TestSQLGenerator
    DDL_GENERATOR_CLASS: Type[BaseDDLGenerator] = _TestDDLGenerator

    @property
    def provider_type(self) -> str:
        return "test"

    @property
    def scalability_tier(self) -> ScalabilityTier:
        return ScalabilityTier.TIER_3_SINGLE_NODE

    def connect(self):
        pass

    def close(self):
        pass

    def execute_sql(self, sql: str, commit: bool = True) -> ExecutionMetrics:
        return ExecutionMetrics(rows_affected=1)

    def bulk_load(
        self,
        source_uri: str,
        target_schema: str,
        target_table: str,
        source_format_options: Dict[str, Any],
        load_options: Dict[str, Any],
    ) -> LoadMetrics:
        return LoadMetrics(rows_inserted=1, rows_rejected=0)


def test_concrete_connection_instantiation():
    """Tests that a correctly implemented concrete class can be instantiated."""
    try:
        conn = ConcreteConnection()
        assert conn is not None
        assert conn.provider_type == "test"
        assert conn.scalability_tier == ScalabilityTier.TIER_3_SINGLE_NODE
    except Exception as e:
        pytest.fail(f"Instantiation of a correct ConcreteConnection failed: {e}")


def test_abc_enforcement():
    """Tests that the ABC enforces implementation of abstract methods."""

    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class IncompleteConnection .* abstract method",
    ):

        class IncompleteConnection(BaseConnection):
            SQL_GENERATOR_CLASS: Type[BaseSQLGenerator] = _TestSQLGenerator
            DDL_GENERATOR_CLASS: Type[BaseDDLGenerator] = _TestDDLGenerator

            def connect(self):
                return super().connect()

        # This line should raise TypeError
        IncompleteConnection()


def test_scalability_tier_enum():
    """Tests the values of the ScalabilityTier enum."""
    assert ScalabilityTier.TIER_1_HORIZONTAL.value == 1
    assert ScalabilityTier.TIER_3_SINGLE_NODE.value == 3
