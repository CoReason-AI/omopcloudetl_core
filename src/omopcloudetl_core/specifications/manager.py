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
from pathlib import Path
from typing import Dict, Optional

import diskcache
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from omopcloudetl_core.exceptions import SpecificationError
from omopcloudetl_core.specifications.models import (
    CDMFieldSpec,
    CDMSpecification,
    CDMTableSpec,
)

# Constants for OHDSI GitHub repository
OHDSI_REPO_URL = "https://raw.githubusercontent.com/OHDSI/CommonDataModel/master/inst/csv"


class SpecificationManager:
    """Manages the fetching, parsing, and caching of OMOP CDM specifications."""

    def __init__(self, cache_dir: Path = None):
        if cache_dir is None:
            cache_dir = Path.home() / ".omopcloudetl_core" / "cache"
        self.cache = diskcache.Cache(str(cache_dir))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _fetch_url_content(self, url: str) -> str:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise SpecificationError(f"Failed to fetch specification data from {url}") from e

    def _parse_cdm_csv(self, csv_content: str) -> Dict[str, CDMTableSpec]:
        """Parses the main CDM specification CSV into a dictionary of table specs."""
        tables: Dict[str, CDMTableSpec] = {}
        reader = csv.DictReader(csv_content.splitlines())
        for row in reader:
            table_name = row["cdmTableName"].lower()
            if table_name not in tables:
                tables[table_name] = CDMTableSpec(
                    name=table_name,
                    fields=[],
                    primary_key=[],  # Will be populated later if available
                )

            # Note: The CSV structure has some optional fields.
            # We handle them gracefully.
            field = CDMFieldSpec(
                name=row["cdmFieldName"].lower(),
                type=row.get("cdmDatatype", "VARCHAR(MAX)"), # Default type if missing
                required=row.get("isRequired", "No").lower() == "yes",
                description=row.get("cdmFieldName"),
            )
            tables[table_name].fields.append(field)

            if row.get("isPrimaryKey", "No").lower() == "yes":
                tables[table_name].primary_key.append(row["cdmFieldName"].lower())

        return tables

    def fetch_specification(self, version: str, local_path: Optional[str] = None) -> CDMSpecification:
        """
        Fetches the OMOP CDM specification for a given version.

        It can load from a local file path or fetch from the official OHDSI
        GitHub repository. Results are cached to avoid redundant processing.

        Args:
            version: The CDM version to fetch (e.g., "5.4"). Used as cache key.
            local_path: An optional path to a local CSV specification file.

        Returns:
            A CDMSpecification object.
        """
        cache_key = f"cdm_spec_{version}_{local_path or 'remote'}"
        cached_spec = self.cache.get(cache_key)
        if cached_spec:
            return cached_spec

        csv_content: str
        if local_path:
            try:
                with open(local_path, "r", encoding="utf-8") as f:
                    csv_content = f.read()
            except FileNotFoundError as e:
                raise SpecificationError(f"Local specification file not found at: {local_path}") from e
        else:
            spec_url = f"{OHDSI_REPO_URL}/OMOP_CDM_v{version}_Field_Level.csv"
            from tenacity import RetryError
            try:
                csv_content = self._fetch_url_content(spec_url)
            except RetryError as e:
                raise SpecificationError(f"Failed to fetch remote specification for version {version}") from e

        try:
            tables = self._parse_cdm_csv(csv_content)
        except Exception as e:
            raise SpecificationError(f"Failed to parse specification for version {version}") from e

        spec = CDMSpecification(version=version, tables=tables)
        self.cache.set(cache_key, spec)
        return spec
