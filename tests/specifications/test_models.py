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
from pydantic import ValidationError
from omopcloudetl_core.specifications.models import (
    CDMSpecification,
    CDMTableSpec,
    CDMFieldSpec,
)

VALID_SPEC = {
    "version": "5.4",
    "tables": {
        "person": {
            "name": "person",
            "fields": [
                {"name": "person_id", "type": "BIGINT", "required": True},
                {"name": "gender_concept_id", "type": "INTEGER", "required": True},
            ],
            "primary_key": ["person_id"],
        }
    },
}


def test_cdm_specification_validation_success():
    """Tests that a valid CDM specification is parsed correctly."""
    spec = CDMSpecification.model_validate(VALID_SPEC)
    assert spec.version == "5.4"
    assert "person" in spec.tables
    assert isinstance(spec.tables["person"], CDMTableSpec)
    assert isinstance(spec.tables["person"].fields[0], CDMFieldSpec)


def test_cdm_table_spec_defaults():
    """Tests that default factory fields are created."""
    table_spec = CDMTableSpec(name="test_table", fields=[], primary_key=[])
    assert table_spec.optimizations == {}
    assert table_spec.foreign_keys == []


def test_cdm_spec_missing_required_field():
    """Tests that validation fails if a required field is missing."""
    invalid_spec = {"version": "5.4"}  # Missing 'tables'
    with pytest.raises(ValidationError):
        CDMSpecification.model_validate(invalid_spec)


def test_cdm_field_spec_missing_type():
    """Tests that validation fails if a field is missing its type."""
    invalid_spec = {
        "version": "5.4",
        "tables": {
            "person": {
                "name": "person",
                "fields": [{"name": "person_id", "required": True}],
                "primary_key": [],
            }
        },
    }
    with pytest.raises(ValidationError):
        CDMSpecification.model_validate(invalid_spec)
