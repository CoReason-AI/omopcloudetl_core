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
from typing import Optional, Dict, List

import diskcache
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from omopcloudetl_core.exceptions import SpecificationError
from omopcloudetl_core.logging import logger
from omopcloudetl_core.specifications.models import (
    CDMFieldSpec,
    CDMSpecification,
    CDMTableSpec,
)


class SpecificationManager:
    """Manages the fetching, parsing, and caching of OMOP CDM specifications."""

    BASE_URL = "https://raw.githubusercontent.com/OHDSI/CommonDataModel/master"

    def __init__(self, cache_dir: str = ".omop_cache"):
        self.cache = diskcache.Cache(cache_dir)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _fetch_url_content(self, url: str) -> str:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch specification from {url}: {e}")
            raise SpecificationError(f"Could not fetch specification data from {url}") from e

    def _parse_specification(self, version: str, field_csv: str, pk_csv: str) -> CDMSpecification:
        try:
            pk_reader = csv.reader(io.StringIO(pk_csv))
            next(pk_reader)  # Skip header
            primary_keys = {row[0].lower(): row[1].lower().split(",") for row in pk_reader}

            field_reader = csv.reader(io.StringIO(field_csv))
            next(field_reader)  # Skip header

            tables: Dict[str, CDMTableSpec] = {}
            for row in field_reader:
                table_name = row[0].lower()
                if table_name not in tables:
                    tables[table_name] = CDMTableSpec(
                        name=table_name,
                        fields=[],
                        primary_key=primary_keys.get(table_name, []),
                    )

                # Handle boolean conversion robustly
                is_required_str = row[2].lower()
                is_required = is_required_str in ("yes", "true", "1")

                field = CDMFieldSpec(
                    name=row[1].lower(),
                    type=row[3].upper(),
                    required=is_required,
                    description=row[8] or None,
                )
                tables[table_name].fields.append(field)

            return CDMSpecification(version=version, tables=tables)
        except Exception as e:
            raise SpecificationError(f"Failed to parse specification for version {version}: {e}") from e

    def fetch_specification(
        self, version: str, local_path: Optional[Path] = None
    ) -> CDMSpecification:
        """
        Fetches a CDM specification, using a cache to avoid repeated downloads.

        Args:
            version: The CDM version to fetch (e.g., "v5.4").
            local_path: An optional path to a directory containing local specification CSVs.

        Returns:
            A parsed CDMSpecification object.
        """
        cache_key = f"cdm_spec_{version}"
        if cache_key in self.cache:
            logger.info(f"Loading CDM specification version {version} from cache.")
            return self.cache[cache_key]

        if local_path:
            logger.info(f"Loading CDM specification from local path: {local_path}")
            field_file = local_path / f"OMOP_CDM_{version}_FIELD_LEVEL.csv"
            pk_file = local_path / f"OMOP_CDM_{version}_Primary_Keys.csv"
            if not field_file.exists() or not pk_file.exists():
                raise SpecificationError(f"Local specification files not found in {local_path}")
            field_csv_content = field_file.read_text()
            pk_csv_content = pk_file.read_text()
        else:
            logger.info(f"Fetching CDM specification version {version} from remote repository.")
            # e.g. OMOP CDM v5.4
            url_version_path = f"OMOP%20CDM%20{version}"
            field_url = f"{self.BASE_URL}/{url_version_path}/OMOP_CDM_{version}_FIELD_LEVEL.csv"
            pk_url = f"{self.BASE_URL}/{url_version_path}/OMOP_CDM_{version}_Primary_Keys.csv"

            field_csv_content = self._fetch_url_content(field_url)
            pk_csv_content = self._fetch_url_content(pk_url)

        spec = self._parse_specification(version, field_csv_content, pk_csv_content)
        self.cache[cache_key] = spec
        return spec
