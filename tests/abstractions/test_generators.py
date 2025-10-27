# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from abc import ABC
from omopcloudetl_core.abstractions.generators import BaseDDLGenerator, BaseSQLGenerator


def test_base_ddl_generator_is_abc():
    """Tests that BaseDDLGenerator is an abstract base class."""
    assert issubclass(BaseDDLGenerator, ABC)


def test_base_sql_generator_is_abc():
    """Tests that BaseSQLGenerator is an abstract base class."""
    assert issubclass(BaseSQLGenerator, ABC)
