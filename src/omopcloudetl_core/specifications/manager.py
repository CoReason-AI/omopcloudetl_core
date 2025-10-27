# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import csv
import io
from pathlib import Path
from typing import Optional

import diskcache as dc
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

from omopcloudetl_core.exceptions import SpecificationError
from omopcloudetl_core.specifications.models import (
    CDMFieldSpec,
    CDMSpecification,
    CDMTableSpec,
)

# Constants
OHDSI_GITHUB_REPO_URL = "https://raw.githubusercontent.com/OHDSI/CommonDataModel/{version}/"


class SpecificationManager:
    """Manages the fetching, caching, and parsing of OMOP CDM specifications."""

    def __init__(self, cache_dir: Path = Path("./.omopcloudetl_cache")):
        """
        Initializes the SpecificationManager.
        Args:
            cache_dir: The directory to use for caching specifications.
        """
        self.cache = dc.Cache(str(cache_dir.resolve()))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _fetch_url(self, url: str) -> str:
        """Fetches content from a URL with retries."""
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            raise SpecificationError(f"Failed to fetch data from {url}: {e}") from e

    def _parse_specification_from_csv(self, version: str, csv_content: str) -> CDMSpecification:
        """Parses the CDM specification CSV into the Pydantic model."""
        tables = {}
        reader = csv.DictReader(io.StringIO(csv_content))

        for row in reader:
            table_name = row["cdmTableName"].lower()
            field_name = row["cdmFieldName"].lower()

            if table_name not in tables:
                tables[table_name] = CDMTableSpec(name=table_name, fields=[], primary_key=[])  # Placeholder for PK

            tables[table_name].fields.append(
                CDMFieldSpec(
                    name=field_name,
                    type=row["cdmDatatype"],
                    required=row["isRequired"].upper() == "YES",
                    description=row.get("cdmSource"),  # Best available field for description
                )
            )
            if row["isPrimaryKey"].upper() == "YES":
                tables[table_name].primary_key.append(field_name)

        return CDMSpecification(version=version, tables=tables)

    def fetch_specification(self, version: str, local_path: Optional[Path] = None) -> CDMSpecification:
        """
        Fetches a CDM specification, using a cache to avoid repeated downloads.

        The order of retrieval is:
        1. Cache
        2. Local file path (if provided)
        3. OHDSI GitHub repository (remote)

        Args:
            version: The version of the CDM specification (e.g., "v5.4").
            local_path: An optional path to a local CSV file for the specification.

        Returns:
            A CDMSpecification object.
        """
        cache_key = f"cdm_spec_{version}"
        cached_spec = self.cache.get(cache_key)
        if cached_spec:
            return cached_spec

        if local_path:
            if not local_path.is_file():
                raise SpecificationError(f"Local specification file not found: {local_path}")
            with open(local_path, "r", encoding="utf-8") as f:
                csv_content = f.read()
        else:
            # The structural definition is typically in a file named after the version
            # e.g., OMOP_CDM_v5.4.csv
            spec_url = f"{OHDSI_GITHUB_REPO_URL.format(version=version)}OMOP_CDM_{version}.csv"
            try:
                csv_content = self._fetch_url(spec_url)
            except RetryError as e:
                raise SpecificationError(f"Failed to fetch data from {spec_url} after multiple retries") from e

        try:
            specification = self._parse_specification_from_csv(version, csv_content)
            self.cache.set(cache_key, specification)
            return specification
        except (KeyError, csv.Error) as e:
            raise SpecificationError(f"Failed to parse CDM specification for version {version}: {e}") from e
