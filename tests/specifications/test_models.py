# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from copy import deepcopy
import pytest
from pydantic import ValidationError
from omopcloudetl_core.specifications.models import CDMSpecification, CDMTableSpec

# A valid, sample CDM specification for testing
VALID_SPEC_DICT = {
    "version": "5.4",
    "tables": {
        "person": {
            "name": "person",
            "fields": [
                {"name": "person_id", "type": "BIGINT", "required": True},
                {"name": "gender_concept_id", "type": "INTEGER", "required": True},
                {"name": "year_of_birth", "type": "INTEGER", "required": True},
                {"name": "person_source_value", "type": "VARCHAR(50)", "required": False},
            ],
            "primary_key": ["person_id"],
            "optimizations": {"databricks": {"zorder_by": ["person_id"]}},
        },
        "observation_period": {
            "name": "observation_period",
            "fields": [
                {"name": "observation_period_id", "type": "BIGINT", "required": True},
                {"name": "person_id", "type": "BIGINT", "required": True},
            ],
            "primary_key": ["observation_period_id"],
        },
    },
}


def test_cdm_specification_valid():
    """Test that a valid CDM specification dictionary can be parsed successfully."""
    spec = CDMSpecification(**VALID_SPEC_DICT)

    assert spec.version == "5.4"
    assert "person" in spec.tables
    assert isinstance(spec.tables["person"], CDMTableSpec)
    assert spec.tables["person"].name == "person"
    assert len(spec.tables["person"].fields) == 4
    assert spec.tables["person"].fields[0].name == "person_id"
    assert spec.tables["person"].fields[0].required is True
    assert spec.tables["person"].primary_key == ["person_id"]
    assert spec.tables["person"].optimizations["databricks"]["zorder_by"] == ["person_id"]


def test_cdm_specification_missing_required_field():
    """Test that validation fails if a required field in the specification is missing."""
    invalid_dict = deepcopy(VALID_SPEC_DICT)
    # 'version' is a required field
    del invalid_dict["version"]
    with pytest.raises(ValidationError):
        CDMSpecification(**invalid_dict)


def test_cdm_table_spec_missing_required_field():
    """Test that validation fails if a required field in a table specification is missing."""
    invalid_dict = deepcopy(VALID_SPEC_DICT)
    # 'fields' is a required field in CDMTableSpec
    del invalid_dict["tables"]["person"]["fields"]
    with pytest.raises(ValidationError):
        CDMSpecification(**invalid_dict)


def test_cdm_field_spec_missing_required_field():
    """Test that validation fails if a required field in a field specification is missing."""
    invalid_dict = deepcopy(VALID_SPEC_DICT)
    # 'type' is a required field in CDMFieldSpec
    del invalid_dict["tables"]["person"]["fields"][0]["type"]
    with pytest.raises(ValidationError):
        CDMSpecification(**invalid_dict)


def test_cdm_table_spec_defaults():
    """Test that default factory fields are correctly initialized."""
    minimal_table_dict = {
        "name": "test_table",
        "fields": [{"name": "id", "type": "INTEGER", "required": True}],
        "primary_key": ["id"],
    }
    table_spec = CDMTableSpec(**minimal_table_dict)
    assert table_spec.foreign_keys == []
    assert table_spec.indexes == []
    assert table_spec.optimizations == {}
