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

VALID_DML = {
    "target_table": "person",
    "target_schema_ref": "cdm",
    "idempotency_keys": ["person_id"],
    "primary_source": {"table": "stg_person", "alias": "p", "schema_ref": "source"},
    "mappings": [
        {"type": "direct", "target_field": "person_id", "source_field": "p.id"},
        {
            "type": "expression",
            "target_field": "gender_concept_id",
            "sql": "CASE p.gender WHEN 'M' THEN 8507 ELSE 8532 END",
        },
        {"type": "constant", "target_field": "source_system", "value": "MySystem"},
    ],
}


def test_dml_definition_validation_success():
    """Tests that a valid DML definition is parsed correctly."""
    dml = DMLDefinition.model_validate(VALID_DML)
    assert dml.target_table == "person"
    assert isinstance(dml.mappings[0], DirectMapping)
    assert isinstance(dml.mappings[1], ExpressionMapping)
    assert isinstance(dml.mappings[2], ConstantMapping)
    assert dml.mappings[0].source_field == "p.id"


def test_dml_discriminator_logic():
    """Tests that the mapping discriminator correctly identifies the mapping type."""
    dml = DMLDefinition.model_validate(VALID_DML)
    assert dml.mappings[1].type == "expression"
    assert dml.mappings[1].sql is not None


def test_dml_invalid_mapping_type():
    """Tests that a DML definition with an invalid mapping type fails validation."""
    invalid_dml = VALID_DML.copy()
    invalid_dml["mappings"] = [{"type": "invalid_type", "target_field": "field"}]
    with pytest.raises(ValidationError):
        DMLDefinition.model_validate(invalid_dml)


def test_dml_missing_required_field_in_mapping():
    """Tests that validation fails if a required field for a mapping type is missing."""
    invalid_dml = VALID_DML.copy()
    invalid_dml["mappings"] = [{"type": "direct", "target_field": "field"}]  # Missing source_field
    with pytest.raises(ValidationError):
        DMLDefinition.model_validate(invalid_dml)
