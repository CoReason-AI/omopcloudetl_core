# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pydantic import BaseModel
from omopcloudetl_core.specifications.models import CDMSpecification, CDMTableSpec, CDMFieldSpec


def test_cdm_specification_is_basemodel():
    """Tests that CDMSpecification is a Pydantic BaseModel."""
    assert issubclass(CDMSpecification, BaseModel)


def test_cdm_table_spec_is_basemodel():
    """Tests that CDMTableSpec is a Pydantic BaseModel."""
    assert issubclass(CDMTableSpec, BaseModel)


def test_cdm_field_spec_is_basemodel():
    """Tests that CDMFieldSpec is a Pydantic BaseModel."""
    assert issubclass(CDMFieldSpec, BaseModel)
