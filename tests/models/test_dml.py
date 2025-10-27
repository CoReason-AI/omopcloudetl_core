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
from omopcloudetl_core.models.dml import (
    DMLDefinition,
    DirectMapping,
    ExpressionMapping,
    ConstantMapping,
)

# A valid, comprehensive DML definition for testing
VALID_DML_DICT = {
    "target_table": "person",
    "target_schema_ref": "cdm_schema",
    "idempotency_keys": ["person_id"],
    "primary_source": {"table": "stg_patients", "alias": "p", "schema_ref": "source_schema"},
    "joins": [
        {
            "target": {"table": "stg_patient_details", "alias": "pd", "schema_ref": "source_schema"},
            "on_condition": "p.patient_id = pd.patient_id",
            "type": "left",
        }
    ],
    "where_clause": "p.is_active = true",
    "mappings": [
        {"type": "direct", "target_field": "person_id", "source_field": "p.patient_id"},
        {
            "type": "expression",
            "target_field": "gender_concept_id",
            "sql": "CASE p.gender WHEN 'M' THEN 8507 ELSE 8532 END",
        },
        {"type": "constant", "target_field": "year_of_birth", "value": 1990},
        {"type": "direct", "target_field": "race_source_value", "source_field": "pd.race"},
    ],
}


def test_dml_definition_valid():
    """Test that a valid DML definition dictionary can be parsed successfully."""
    dml_def = DMLDefinition(**VALID_DML_DICT)

    assert dml_def.target_table == "person"
    assert dml_def.primary_source.alias == "p"
    assert len(dml_def.joins) == 1
    assert dml_def.joins[0].type == "left"
    assert len(dml_def.mappings) == 4

    # Test discriminated union parsing
    assert isinstance(dml_def.mappings[0], DirectMapping)
    assert dml_def.mappings[0].source_field == "p.patient_id"

    assert isinstance(dml_def.mappings[1], ExpressionMapping)
    assert "CASE p.gender" in dml_def.mappings[1].sql

    assert isinstance(dml_def.mappings[2], ConstantMapping)
    assert dml_def.mappings[2].value == 1990


def test_dml_definition_missing_required_fields():
    """Test that validation fails if required fields in the root model are missing."""
    invalid_dict = VALID_DML_DICT.copy()
    del invalid_dict["primary_source"]  # Remove a required field
    with pytest.raises(ValidationError):
        DMLDefinition(**invalid_dict)


def test_mapping_validation_fails_on_bad_type():
    """Test that validation fails if a mapping has an unknown type."""
    invalid_dict = VALID_DML_DICT.copy()
    invalid_dict["mappings"] = [{"type": "unknown_type", "target_field": "field1"}]
    with pytest.raises(ValidationError):
        DMLDefinition(**invalid_dict)


def test_mapping_validation_fails_on_missing_field():
    """Test that validation fails if a mapping is missing a required field."""
    invalid_dict = VALID_DML_DICT.copy()
    # DirectMapping is missing 'source_field'
    invalid_dict["mappings"] = [{"type": "direct", "target_field": "field1"}]
    with pytest.raises(ValidationError):
        DMLDefinition(**invalid_dict)


def test_dml_definition_defaults():
    """Test that default values (e.g., empty joins list) are correctly applied."""
    minimal_dict = {
        "target_table": "person",
        "target_schema_ref": "cdm",
        "idempotency_keys": ["person_id"],
        "primary_source": {"table": "stg_patients", "alias": "p", "schema_ref": "source"},
        "mappings": [{"type": "direct", "target_field": "person_id", "source_field": "p.patient_id"}],
    }
    dml_def = DMLDefinition(**minimal_dict)
    assert dml_def.joins == []
    assert dml_def.where_clause is None
