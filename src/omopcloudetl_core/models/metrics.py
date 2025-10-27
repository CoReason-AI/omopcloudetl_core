# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Optional

from pydantic import BaseModel


class LoadMetrics(BaseModel):
    """Metrics for bulk load operations (e.g., COPY INTO)."""

    rows_processed: Optional[int] = None
    rows_inserted: int
    rows_rejected: int
    error_details_uri: Optional[str] = None  # Link to a file with rejected records
    query_id: Optional[str] = None


class ExecutionMetrics(BaseModel):
    """Metrics for DML/SQL execution (e.g., MERGE, INSERT, DDL)."""

    rows_affected: Optional[int] = None  # Total rows inserted, updated, or deleted
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    query_id: Optional[str] = None
