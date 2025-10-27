# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from datetime import datetime, timezone
from typing import Optional, Union
from uuid import UUID

from omopcloudetl_core.abstractions.connections import BaseConnection
from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics


class MetadataManager:
    """Manages the creation and updating of execution metadata."""

    def __init__(self, connection: BaseConnection, schema: str, table_name: str = "omopcloudetl_execution_log"):
        self.connection = connection
        self.schema = schema
        self.table_name = table_name
        self.qualified_table_name = f"{self.schema}.{self.table_name}"
        # Use a placeholder style appropriate for most DBAPIs (e.g., qmark or format).
        # Concrete connection implementations would ideally expose this.
        self.param_style = "?"

    def initialize_store(self) -> None:
        """Create the metadata table if it doesn't exist."""
        # This DDL is generic; specific dialects might need adjustments
        # (e.g., for auto-incrementing primary keys).
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.qualified_table_name} (
            log_id BIGINT PRIMARY KEY,
            execution_id VARCHAR(36) NOT NULL,
            step_name VARCHAR(255) NOT NULL,
            status VARCHAR(50),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds BIGINT,
            rows_affected BIGINT,
            rows_processed BIGINT,
            rows_inserted BIGINT,
            rows_rejected BIGINT,
            error_details_uri VARCHAR(1024),
            query_id VARCHAR(255),
            error_message TEXT,
            last_updated TIMESTAMP
        );
        """
        # DDL statements generally don't support parameters, so direct execution is acceptable.
        self.connection.execute_sql(ddl)

    def log_step_start(self, execution_id: UUID, step_name: str, log_id: int) -> None:
        """Logs the start of a workflow step."""
        sql = f"""
        INSERT INTO {self.qualified_table_name}
        (log_id, execution_id, step_name, status, start_time, last_updated)
        VALUES ({self.param_style}, {self.param_style}, {self.param_style}, 'RUNNING', {self.param_style}, {self.param_style})
        """
        params = [
            log_id,
            str(execution_id),
            step_name,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        ]
        self.connection.execute_sql(sql, params=params)

    def log_step_end(
        self,
        log_id: int,
        status: str,
        metrics: Optional[Union[LoadMetrics, ExecutionMetrics]] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Logs the completion or failure of a workflow step."""
        # Use COALESCE to avoid overwriting existing values with NULL if a field is not applicable
        sql = f"""
        UPDATE {self.qualified_table_name}
        SET
            status = {self.param_style},
            end_time = {self.param_style},
            duration_seconds = CAST(EXTRACT(EPOCH FROM ({self.param_style} - start_time)) AS BIGINT),
            rows_affected = COALESCE({self.param_style}, rows_affected),
            rows_processed = COALESCE({self.param_style}, rows_processed),
            rows_inserted = COALESCE({self.param_style}, rows_inserted),
            rows_rejected = COALESCE({self.param_style}, rows_rejected),
            error_details_uri = COALESCE({self.param_style}, error_details_uri),
            query_id = COALESCE({self.param_style}, query_id),
            error_message = COALESCE({self.param_style}, error_message),
            last_updated = {self.param_style}
        WHERE log_id = {self.param_style}
        """

        end_time = datetime.now(timezone.utc)
        params = [
            status,
            end_time,
            end_time,
            metrics.rows_affected if isinstance(metrics, ExecutionMetrics) else None,
            metrics.rows_processed if isinstance(metrics, LoadMetrics) else None,
            metrics.rows_inserted if metrics else None,
            metrics.rows_rejected if isinstance(metrics, LoadMetrics) else None,
            metrics.error_details_uri if isinstance(metrics, LoadMetrics) else None,
            metrics.query_id if metrics else None,
            error_message,
            end_time,
            log_id,
        ]
        self.connection.execute_sql(sql, params=params)
