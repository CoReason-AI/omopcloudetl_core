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
from omopcloudetl_core.models.dml import DMLDefinition


@pytest.fixture
def valid_dml_data():
    """Provides a valid DML definition as a dictionary."""
    return {
        "target_table": "person",
        "target_schema_ref": "cdm_schema",
        "idempotency_keys": ["person_id"],
        "primary_source": {"table": "source_patient", "alias": "p", "schema_ref": "source_schema"},
        "joins": [
            {
                "target": {"table": "source_gender", "alias": "g", "schema_ref": "source_schema"},
                "on_condition": "p.gender_id = g.id",
                "type": "left",
            }
        ],
        "where_clause": "p.is_active = true",
        "mappings": [
            {"type": "direct", "target_field": "person_id", "source_field": "p.patient_id"},
            {
                "type": "expression",
                "target_field": "gender_concept_id",
                "sql": "CASE g.name WHEN 'MALE' THEN 8507 ELSE 8532 END",
            },
            {"type": "constant", "target_field": "person_source_value", "value": "SOURCE_DB_1"},
        ],
    }


def test_dml_definition_valid(valid_dml_data):
    """Tests successful creation of DMLDefinition from valid data."""
    dml = DMLDefinition(**valid_dml_data)
    assert dml.target_table == "person"
    assert dml.primary_source.alias == "p"
    assert len(dml.joins) == 1
    assert dml.joins[0].type == "left"
    assert len(dml.mappings) == 3
    assert dml.mappings[0].type == "direct"
    assert dml.mappings[1].type == "expression"
    assert dml.mappings[2].type == "constant"


def test_dml_definition_invalid_mapping_type(valid_dml_data):
    """Tests that an invalid mapping type raises a ValidationError."""
    valid_dml_data["mappings"].append({"type": "invalid_type", "target_field": "field"})
    with pytest.raises(ValidationError):
        DMLDefinition(**valid_dml_data)


def test_dml_definition_missing_required_field(valid_dml_data):
    """Tests that missing a required field raises a ValidationError."""
    del valid_dml_data["primary_source"]
    with pytest.raises(ValidationError):
        DMLDefinition(**valid_dml_data)


def test_dml_definition_empty_joins_and_where(valid_dml_data):
    """Tests that a DML definition is valid without optional joins and where clause."""
    del valid_dml_data["joins"]
    del valid_dml_data["where_clause"]
    dml = DMLDefinition(**valid_dml_data)
    assert len(dml.joins) == 0
    assert dml.where_clause is None
