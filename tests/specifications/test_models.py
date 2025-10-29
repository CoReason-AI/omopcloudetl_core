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
                {"name": "person_id", "type": "BIGINT", "required": True, "description": "An ID for each person."},
                {"name": "gender_concept_id", "type": "INTEGER", "required": True},
            ],
            "primary_key": ["person_id"],
            "foreign_keys": [{"name": "fk_gender", "references": "concept", "on": "gender_concept_id"}],
            "indexes": [{"name": "idx_person_id", "columns": ["person_id"]}],
            "optimizations": {"databricks": {"zorder_by": ["person_id"]}},
        }
    },
}


def test_cdm_specification_validation_success():
    """Tests that a valid CDM specification is parsed correctly."""
    spec = CDMSpecification.model_validate(VALID_SPEC)
    assert spec.version == "5.4"
    assert "person" in spec.tables
    person_table = spec.tables["person"]
    assert isinstance(person_table, CDMTableSpec)
    assert person_table.name == "person"
    assert len(person_table.fields) == 2
    assert isinstance(person_table.fields[0], CDMFieldSpec)
    assert person_table.fields[0].description == "An ID for each person."
    assert person_table.fields[1].description is None
    assert person_table.primary_key == ["person_id"]
    assert len(person_table.foreign_keys) == 1
    assert person_table.foreign_keys[0]["name"] == "fk_gender"
    assert len(person_table.indexes) == 1
    assert person_table.indexes[0]["name"] == "idx_person_id"
    assert "databricks" in person_table.optimizations


def test_cdm_table_spec_defaults():
    """Tests that default factory fields are created."""
    table_spec = CDMTableSpec(name="test_table", fields=[], primary_key=[])
    assert table_spec.optimizations == {}
    assert table_spec.foreign_keys == []
    assert table_spec.indexes == []


def test_cdm_spec_missing_required_field():
    """Tests that validation fails if a required field is missing."""
    invalid_spec = {"version": "5.4"}  # Missing 'tables'
    with pytest.raises(ValidationError) as exc_info:
        CDMSpecification.model_validate(invalid_spec)
    assert "tables" in str(exc_info.value)


def test_cdm_table_spec_missing_required_field():
    """Tests validation fails if a required field in CDMTableSpec is missing."""
    with pytest.raises(ValidationError) as exc_info:
        # Missing 'fields'
        CDMTableSpec(name="test_table", primary_key=[])
    assert "fields" in str(exc_info.value)


def test_cdm_field_spec_missing_type():
    """Tests that validation fails if a field is missing its type."""
    invalid_spec = {
        "version": "5.4",
        "tables": {
            "person": {
                "name": "person",
                "fields": [{"name": "person_id", "required": True}],  # Missing type
                "primary_key": [],
            }
        },
    }
    with pytest.raises(ValidationError) as exc_info:
        CDMSpecification.model_validate(invalid_spec)
    assert "type" in str(exc_info.value)
    assert "person.fields.0" in str(exc_info.value)  # check error location


def test_cdm_spec_with_empty_tables():
    """Tests that a spec with an empty tables dictionary is valid."""
    spec_data = {"version": "5.4", "tables": {}}
    spec = CDMSpecification.model_validate(spec_data)
    assert spec.version == "5.4"
    assert spec.tables == {}


def test_cdm_spec_tables_not_a_dict():
    """Tests validation fails if 'tables' is not a dictionary."""
    invalid_spec = {"version": "5.4", "tables": ["table1", "table2"]}
    with pytest.raises(ValidationError) as exc_info:
        CDMSpecification.model_validate(invalid_spec)
    assert "tables" in str(exc_info.value)
    assert "Input should be a valid dictionary" in str(exc_info.value)
