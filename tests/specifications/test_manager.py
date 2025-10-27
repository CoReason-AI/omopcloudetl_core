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
from pathlib import Path
import requests
from unittest.mock import MagicMock

from omopcloudetl_core.exceptions import SpecificationError
from omopcloudetl_core.specifications.manager import SpecificationManager, OHDSI_GITHUB_REPO_URL

# Sample CSV content mimicking the OHDSI specification format
SAMPLE_CSV_CONTENT = """cdmTableName,cdmFieldName,isPrimaryKey,isRequired,cdmDatatype,cdmSource
PERSON,PERSON_ID,Yes,Yes,BIGINT,person.person_id
PERSON,GENDER_CONCEPT_ID,No,Yes,INTEGER,person.gender_concept_id
VISIT_OCCURRENCE,VISIT_OCCURRENCE_ID,Yes,Yes,BIGINT,visit_occurrence.visit_occurrence_id
VISIT_OCCURRENCE,PERSON_ID,No,Yes,BIGINT,visit_occurrence.person_id
"""

VERSION = "v5.4"
BASE_URL = OHDSI_GITHUB_REPO_URL.format(version=VERSION)
SPEC_URL = f"{BASE_URL}OMOP_CDM_{VERSION}.csv"


@pytest.fixture
def manager(tmp_path: Path) -> SpecificationManager:
    """Provides a SpecificationManager instance with a temporary cache directory."""
    return SpecificationManager(cache_dir=tmp_path / "cache")


def test_fetch_specification_remote_success(manager: SpecificationManager, requests_mock):
    """Test successful fetching and parsing of a remote specification."""
    requests_mock.get(SPEC_URL, text=SAMPLE_CSV_CONTENT)

    spec = manager.fetch_specification(VERSION)

    assert spec.version == VERSION
    assert "person" in spec.tables
    assert "visit_occurrence" in spec.tables
    assert len(spec.tables["person"].fields) == 2
    assert spec.tables["person"].primary_key == ["person_id"]
    assert spec.tables["person"].fields[0].name == "person_id"
    assert spec.tables["person"].fields[0].type == "BIGINT"
    assert spec.tables["person"].fields[0].required is True


def test_fetch_specification_local_success(manager: SpecificationManager, tmp_path: Path):
    """Test successful fetching and parsing from a local file."""
    local_file = tmp_path / "local_spec.csv"
    local_file.write_text(SAMPLE_CSV_CONTENT)

    spec = manager.fetch_specification(VERSION, local_path=local_file)

    assert spec.version == VERSION
    assert "person" in spec.tables
    assert len(spec.tables["person"].fields) == 2


def test_fetch_specification_caching(manager: SpecificationManager, requests_mock):
    """Test that specifications are cached after the first fetch."""
    requests_mock.get(SPEC_URL, text=SAMPLE_CSV_CONTENT)

    # First call - should fetch from URL
    spec1 = manager.fetch_specification(VERSION)
    assert requests_mock.call_count == 1

    # Second call - should use cache, not call the URL again
    spec2 = manager.fetch_specification(VERSION)
    assert requests_mock.call_count == 1
    assert spec1 == spec2


def test_fetch_specification_remote_http_error(manager: SpecificationManager, requests_mock):
    """Test that a SpecificationError is raised on HTTP error."""
    requests_mock.get(SPEC_URL, status_code=404, reason="Not Found")

    with pytest.raises(SpecificationError, match="Failed to fetch data"):
        manager.fetch_specification(VERSION)


def test_fetch_specification_remote_network_error(manager: SpecificationManager, requests_mock):
    """Test that a SpecificationError is raised on network error."""
    requests_mock.get(SPEC_URL, exc=requests.exceptions.ConnectTimeout)

    with pytest.raises(SpecificationError, match="Failed to fetch data"):
        manager.fetch_specification(VERSION)


def test_fetch_specification_local_file_not_found(manager: SpecificationManager, tmp_path: Path):
    """Test that a SpecificationError is raised for a missing local file."""
    with pytest.raises(SpecificationError, match="Local specification file not found"):
        manager.fetch_specification(VERSION, local_path=tmp_path / "non_existent.csv")


def test_parse_invalid_csv_data(manager: SpecificationManager, requests_mock):
    """Test that a SpecificationError is raised for malformed CSV."""
    malformed_csv = "header1,header2\nincomplete_row"
    requests_mock.get(SPEC_URL, text=malformed_csv)

    with pytest.raises(SpecificationError, match="Failed to parse CDM specification"):
        manager.fetch_specification(VERSION)


def test_parse_missing_csv_column(manager: SpecificationManager, requests_mock):
    """Test parsing CSV with a missing required column raises an error."""
    missing_column_csv = "cdmFieldName,isPrimaryKey,isRequired\nFIELD1,Yes,No"
    requests_mock.get(SPEC_URL, text=missing_column_csv)

    with pytest.raises(SpecificationError, match="Failed to parse CDM specification"):
        manager.fetch_specification(VERSION)

def test_cache_is_used(manager: SpecificationManager, mocker):
    """Verify that the diskcache is actually used."""
    mock_cache = MagicMock()
    mock_cache.get.return_value = "cached_value"
    mocker.patch.object(manager, "cache", mock_cache)

    result = manager.fetch_specification(VERSION)

    mock_cache.get.assert_called_once_with(f"cdm_spec_{VERSION}")
    assert result == "cached_value"
