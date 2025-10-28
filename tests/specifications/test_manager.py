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
from omopcloudetl_core.specifications.manager import SpecificationManager
from omopcloudetl_core.exceptions import SpecificationError


@pytest.fixture
def spec_manager(tmp_path):
    """Fixture for a SpecificationManager with a temporary cache directory."""
    return SpecificationManager(cache_dir=str(tmp_path / ".cache"))


def test_fetch_specification_remote_success(spec_manager, requests_mock):
    """Test successful fetching of a specification from a remote URL."""
    version = "v5.4"
    field_url = f"{spec_manager.BASE_URL}/OMOP%20CDM%20{version}/OMOP_CDM_{version}_FIELD_LEVEL.csv"
    pk_url = f"{spec_manager.BASE_URL}/OMOP%20CDM%20{version}/OMOP_CDM_{version}_Primary_Keys.csv"

    field_csv = "cdm_table_name,cdm_field_name,is_required,cdm_datatype,is_deprecated,numerical_precision,source_value,range,description"
    pk_csv = "table_name,constraint_name,constraint_type"

    requests_mock.get(field_url, text=field_csv)
    requests_mock.get(pk_url, text=pk_csv)

    spec = spec_manager.fetch_specification(version)
    assert spec.version == version
    assert spec.tables == {}


def test_fetch_specification_remote_failure(spec_manager, requests_mock):
    """Test failure when fetching a specification from a remote URL."""
    version = "v5.4"
    field_url = f"{spec_manager.BASE_URL}/OMOP%20CDM%20{version}/OMOP_CDM_{version}_FIELD_LEVEL.csv"
    requests_mock.get(field_url, status_code=404)

    with pytest.raises(SpecificationError):
        spec_manager.fetch_specification(version)


def test_fetch_specification_local_success(spec_manager, tmp_path):
    """Test successful fetching of a specification from a local path."""
    version = "v5.4"
    local_path = tmp_path
    (local_path / f"OMOP_CDM_{version}_FIELD_LEVEL.csv").write_text(
        "cdm_table_name,cdm_field_name,is_required,cdm_datatype,is_deprecated,numerical_precision,source_value,range,description"
    )
    (local_path / f"OMOP_CDM_{version}_Primary_Keys.csv").write_text("table_name,constraint_name,constraint_type")

    spec = spec_manager.fetch_specification(version, local_path=local_path)
    assert spec.version == version
    assert spec.tables == {}


def test_fetch_specification_local_failure(spec_manager, tmp_path):
    """Test failure when local specification files are not found."""
    version = "v5.4"
    with pytest.raises(SpecificationError):
        spec_manager.fetch_specification(version, local_path=tmp_path)


def test_parsing_logic(spec_manager):
    """Test the CSV parsing and model hydration logic."""
    version = "v5.4"
    field_csv = (
        "cdm_table_name,cdm_field_name,is_required,cdm_datatype,is_deprecated,numerical_precision,source_value,range,description\n"
        "person,person_id,Yes,BIGINT,No,0,,,Person identifier\n"
        "person,gender_concept_id,No,INTEGER,No,0,,,Gender concept identifier"
    )
    pk_csv = "table_name,constraint_name,constraint_type\nperson,pk_person,PRIMARY KEY"

    spec = spec_manager._parse_specification(version, field_csv, pk_csv)

    assert "person" in spec.tables
    person_table = spec.tables["person"]
    assert person_table.name == "person"
    assert len(person_table.fields) == 2
    assert person_table.primary_key == ["pk_person"]

    field1 = person_table.fields[0]
    assert field1.name == "person_id"
    assert field1.type == "BIGINT"
    assert field1.required is True
    assert field1.description == "Person identifier"

    field2 = person_table.fields[1]
    assert field2.name == "gender_concept_id"
    assert field2.type == "INTEGER"
    assert field2.required is False
