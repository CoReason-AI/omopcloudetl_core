# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from datetime import datetime
from uuid import UUID
from typing import Union

from omopcloudetl_core.abstractions.connections import BaseConnection
from omopcloudetl_core.models.metrics import ExecutionMetrics, LoadMetrics
from omopcloudetl_core.logging import logger

class MetadataManager:
    """Manages logging of workflow execution metadata to a database table."""

    TABLE_NAME = "omopcloudetl_execution_log"

    def __init__(self, connection: BaseConnection, execution_id: UUID):
        self.connection = connection
        self.execution_id = execution_id

    def initialize_store(self) -> None:
        """Creates the metadata table if it does not exist."""
        logger.info(f"Initializing metadata store: ensuring table '{self.TABLE_NAME}' exists.")

        # This DDL is intended to be generic. Specific dialects might need adjustments.
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
            execution_id VARCHAR(36) NOT NULL,
            step_name VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds DECIMAL(18, 4),
            query_id VARCHAR(255),
            rows_affected BIGINT,
            rows_inserted BIGINT,
            rows_updated BIGINT,
            rows_deleted BIGINT,
            rows_processed BIGINT,
            rows_rejected BIGINT,
            error_details_uri VARCHAR(1024),
            error_message TEXT
        );
        """
        try:
            self.connection.execute_sql(ddl)
            logger.info(f"Metadata table '{self.TABLE_NAME}' is ready.")
        except Exception as e:
            logger.error(f"Failed to initialize metadata store: {e}")
            raise

    def log_step_start(self, step_name: str) -> None:
        """Logs the start of a workflow step."""
        start_time = datetime.utcnow()
        sql = f"""
        INSERT INTO {self.TABLE_NAME} (execution_id, step_name, status, start_time)
        VALUES (?, ?, ?, ?);
        """
        self.connection.execute_sql(sql, params=[str(self.execution_id), step_name, "RUNNING", start_time])

    def log_step_end(
        self,
        step_name: str,
        status: str,
        metrics: Union[ExecutionMetrics, LoadMetrics, None] = None,
        error_message: str | None = None,
    ) -> None:
        """Logs the end of a workflow step, updating its status and metrics."""
        end_time = datetime.utcnow()

        # Use database functions to calculate duration for accuracy
        duration_calc = "EXTRACT(EPOCH FROM (end_time - start_time))"

        set_clauses = [
            "status = ?",
            "end_time = ?",
            f"duration_seconds = {duration_calc}",
        ]
        params = [status, end_time]

        if metrics:
            metric_fields = metrics.model_dump(exclude_none=True)
            for key, value in metric_fields.items():
                set_clauses.append(f"{key} = ?")
                params.append(value)

        if error_message:
            set_clauses.append("error_message = ?")
            params.append(error_message)

        sql = f"""
        UPDATE {self.TABLE_NAME}
        SET {', '.join(set_clauses)}
        WHERE execution_id = ? AND step_name = ? AND status = 'RUNNING';
        """
        params.extend([str(self.execution_id), step_name])

        self.connection.execute_sql(sql, params=params)
