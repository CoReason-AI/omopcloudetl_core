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
from abc import ABC
from typing import Any, Dict, List

from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator
from omopcloudetl_core.models.dml import DMLDefinition
from omopcloudetl_core.specifications.models import CDMSpecification


def test_base_ddl_generator_is_abc():
    """Tests that BaseDDLGenerator is an abstract base class."""
    assert issubclass(BaseDDLGenerator, ABC)


def test_base_sql_generator_is_abc():
    """Tests that BaseSQLGenerator is an abstract base class."""
    assert issubclass(BaseSQLGenerator, ABC)


def test_ddl_generator_abc_enforcement():
    """Tests that the ABC enforces implementation of `generate_ddl`."""
    with pytest.raises(TypeError, match="Can't instantiate abstract class IncompleteDDL"):

        class IncompleteDDL(BaseDDLGenerator):
            pass

        IncompleteDDL()

    # Test that a complete implementation works
    class CompleteDDL(BaseDDLGenerator):
        def generate_ddl(self, specification: CDMSpecification, schema_name: str, options: dict) -> List[str]:
            return ["CREATE TABLE foo;"]

    assert CompleteDDL() is not None


def test_sql_generator_abc_enforcement():
    """Tests that the ABC enforces implementation of `generate_transform_sql`."""
    with pytest.raises(TypeError, match="Can't instantiate abstract class IncompleteSQL"):

        class IncompleteSQL(BaseSQLGenerator):
            pass

        IncompleteSQL()

    # Test that a complete implementation works
    class CompleteSQL(BaseSQLGenerator):
        def generate_transform_sql(self, dml_definition: DMLDefinition, context: Dict[str, Any]) -> str:
            return "SELECT 1;"

    assert CompleteSQL() is not None
