# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pathlib import Path

import pytest
import requests_mock
from omopcloudetl_core.exceptions import SpecificationError
from omopcloudetl_core.specifications.manager import (
    OHDSI_REPO_URL,
    SpecificationManager,
)

# A simplified but representative sample of the OHDSI specification CSV
SAMPLE_CDM_CSV = """cdmTableName,cdmFieldName,isRequired,isPrimaryKey,cdmDatatype
person,person_id,Yes,Yes,INTEGER
person,gender_concept_id,Yes,No,INTEGER
visit_occurrence,visit_occurrence_id,Yes,Yes,INTEGER
"""


@pytest.fixture
def spec_manager(tmp_path: Path) -> SpecificationManager:
    """Fixture to provide a SpecificationManager with a temporary cache directory."""
    cache_dir = tmp_path / "cache"
    return SpecificationManager(cache_dir=cache_dir)


def test_fetch_specification_from_remote_success(
    spec_manager: SpecificationManager, requests_mock: requests_mock.Mocker
):
    """Test successful fetching and parsing of a specification from a remote URL."""
    version = "5.4"
    spec_url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    requests_mock.get(spec_url, text=SAMPLE_CDM_CSV)

    spec = spec_manager.fetch_specification(version)

    assert spec.version == version
    assert "person" in spec.tables
    assert len(spec.tables["person"].fields) == 2


def test_fetch_specification_from_local_path_success(spec_manager: SpecificationManager, tmp_path: Path):
    """Test successful loading and parsing of a specification from a local file."""
    version = "local_v1"
    local_file = tmp_path / "local_spec.csv"
    local_file.write_text(SAMPLE_CDM_CSV)

    spec = spec_manager.fetch_specification(version, local_path=str(local_file))

    assert spec.version == version
    assert "person" in spec.tables
    assert "visit_occurrence" in spec.tables


def test_fetch_specification_local_file_not_found(spec_manager: SpecificationManager):
    """Test that a SpecificationError is raised if the local file does not exist."""
    with pytest.raises(SpecificationError, match="Local specification file not found"):
        spec_manager.fetch_specification("local_v1", local_path="non_existent_file.csv")


def test_fetch_specification_caching(spec_manager: SpecificationManager, requests_mock: requests_mock.Mocker):
    """Test that a fetched specification is cached."""
    version = "5.4"
    spec_url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    requests_mock.get(spec_url, text=SAMPLE_CDM_CSV)

    # First call - should fetch from URL and cache
    spec_manager.fetch_specification(version)
    assert requests_mock.call_count == 1

    # Second call - should load from cache
    spec_manager.fetch_specification(version)
    assert requests_mock.call_count == 1  # No new HTTP request


def test_fetch_specification_http_error(spec_manager: SpecificationManager, requests_mock: requests_mock.Mocker):
    """Test that a SpecificationError is raised on HTTP failure."""
    version = "5.4"
    spec_url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    requests_mock.get(spec_url, status_code=404, text="Not Found")

    with pytest.raises(SpecificationError, match="Failed to fetch remote specification"):
        spec_manager.fetch_specification(version)


def test_fetch_specification_parsing_error(spec_manager: SpecificationManager, requests_mock: requests_mock.Mocker):
    """Test that a SpecificationError is raised on CSV parsing failure."""
    version = "5.4"
    spec_url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
    # Malformed CSV missing the required 'cdmTableName' header
    malformed_csv = "wrongHeader,cdmFieldName,isRequired\nval1,val2,val3"
    requests_mock.get(spec_url, text=malformed_csv)

    with pytest.raises(SpecificationError, match="Failed to parse specification"):
        spec_manager.fetch_specification(version)


def test_specification_manager_custom_cache_dir(tmp_path: Path):
    """Test that the SpecificationManager correctly uses a custom cache directory."""
    custom_cache = tmp_path / "custom_cache"
    manager = SpecificationManager(cache_dir=custom_cache)
    assert manager.cache.directory == str(custom_cache)


def test_specification_manager_default_cache_dir():
    """Test that the SpecificationManager defaults to a cache in the home directory."""
    # This test is a bit indirect, but it exercises the 'cache_dir is None' branch
    # without making assumptions about the runner's home directory structure.
    manager = SpecificationManager()
    assert ".omopcloudetl_core" in manager.cache.directory
