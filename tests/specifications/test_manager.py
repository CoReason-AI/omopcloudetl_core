# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import os
import pytest
import requests
from pathlib import Path
from unittest.mock import patch, mock_open
from tenacity import RetryError
from omopcloudetl_core.specifications.manager import SpecificationManager, OHDSI_REPO_URL
from omopcloudetl_core.exceptions import SpecificationError

VALID_CDM_CSV = (
    "cdmTableName,cdmFieldName,isPrimaryKey,isRequired,cdmDatatype,description\n"
    "PERSON,PERSON_ID,Yes,Yes,BIGINT,A unique identifier for each person.\n"
    "PERSON,GENDER_CONCEPT_ID,No,Yes,INTEGER,A foreign key to the GENDER concept in the Standardized Vocabularies."
)


@pytest.fixture
def spec_manager(tmp_path):
    """Provides a SpecificationManager instance with a temporary cache directory."""
    return SpecificationManager(cache_dir=tmp_path)


def test_spec_manager_init_no_cache_dir(monkeypatch):
    """Tests that the SpecificationManager initializes without a cache_dir."""
    # os.path.join is used to create a platform-independent path
    expected_path = os.path.join(os.path.expanduser("~"), ".omopcloudetl_core", "cache")
    monkeypatch.setattr(Path, "home", lambda: Path(os.path.expanduser("~")))
    manager = SpecificationManager()
    assert manager.cache.directory == expected_path


def test_fetch_specification_from_remote_success(spec_manager, requests_mock):
    """Tests successfully fetching and parsing a remote CDM specification."""
    version = "5.4"
    url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    requests_mock.get(url, text=VALID_CDM_CSV)

    spec = spec_manager.fetch_specification(version)

    assert spec.version == version
    assert "person" in spec.tables
    assert spec.tables["person"].fields[0].name == "person_id"
    assert spec.tables["person"].primary_key == ["person_id"]
    assert spec.tables["person"].fields[0].description == "A unique identifier for each person."


def test_parse_cdm_csv_parses_description(spec_manager):
    """Tests that the parser correctly extracts the description field."""
    tables = spec_manager._parse_cdm_csv(VALID_CDM_CSV)
    assert "person" in tables
    person_fields = {field.name: field for field in tables["person"].fields}
    assert person_fields["person_id"].description == "A unique identifier for each person."
    assert (
        person_fields["gender_concept_id"].description
        == "A foreign key to the GENDER concept in the Standardized Vocabularies."
    )


def test_fetch_specification_from_local_file_success(spec_manager):
    """Tests successfully loading a specification from a local file."""
    version = "5.4-local"
    local_path = "/fake/path/spec.csv"
    with patch("builtins.open", mock_open(read_data=VALID_CDM_CSV)):
        spec = spec_manager.fetch_specification(version, local_path=local_path)
        assert "person" in spec.tables


def test_fetch_specification_caching(spec_manager, requests_mock):
    """Tests that specifications are cached after the first fetch."""
    version = "5.5"
    url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    requests_mock.get(url, text=VALID_CDM_CSV)

    spec_manager.fetch_specification(version)  # First call, should fetch and cache
    spec_manager.fetch_specification(version)  # Second call, should use cache

    assert requests_mock.call_count == 1


def test_fetch_remote_fails_with_network_error(spec_manager, requests_mock):
    """Tests that SpecificationError is raised on a network failure."""
    version = "5.6"
    url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    requests_mock.get(url, exc=requests.exceptions.RequestException("Test error"))

    with pytest.raises(SpecificationError, match="Failed to fetch remote"):
        spec_manager.fetch_specification(version)


def test_fetch_local_fails_with_file_not_found(spec_manager):
    """Tests that SpecificationError is raised if the local file does not exist."""
    with pytest.raises(SpecificationError, match="not found"):
        # The mock_open patch is omitted, so open will raise FileNotFoundError
        spec_manager.fetch_specification("5.4", local_path="/non/existent/file.csv")


def test_fetch_remote_fails_after_retries(spec_manager):
    """Tests that SpecificationError is raised after all retry attempts fail."""
    with patch(
        "omopcloudetl_core.specifications.manager.SpecificationManager._fetch_url_content",
        side_effect=RetryError(last_attempt="..."),
    ):
        with pytest.raises(SpecificationError, match="Failed to fetch remote"):
            spec_manager.fetch_specification("5.7")


def test_parsing_fails_with_generic_error(spec_manager):
    """Tests that a generic error during parsing is wrapped in SpecificationError."""
    with patch("builtins.open", mock_open(read_data=VALID_CDM_CSV)):
        with patch(
            "omopcloudetl_core.specifications.manager.SpecificationManager._parse_cdm_csv",
            side_effect=Exception("Parsing error"),
        ):
            with pytest.raises(SpecificationError, match="Failed to parse"):
                spec_manager.fetch_specification("5.8", local_path="/fake/path/spec.csv")
