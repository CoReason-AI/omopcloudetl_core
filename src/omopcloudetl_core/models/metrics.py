# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pydantic import BaseModel
from typing import Optional

class LoadMetrics(BaseModel):
    """Metrics for bulk load operations (COPY INTO, etc.)."""
    rows_processed: Optional[int] = None
    rows_inserted: int
    rows_rejected: int
    error_details_uri: Optional[str] = None # Link to rejected records
    query_id: Optional[str] = None

class ExecutionMetrics(BaseModel):
    """Metrics for DML/SQL execution (MERGE, INSERT, DDL)."""
    rows_affected: Optional[int] = None # Inserted + Updated + Deleted
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    query_id: Optional[str] = None
