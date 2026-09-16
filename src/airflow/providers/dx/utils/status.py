# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from typing import Any, Literal

from airflow.providers.dx.consts import (
    FAILURE_JOB_STATUSES,
    LOCATION_STATUS_ONLINE,
    LOCATION_STATUS_WARNING,
    SUCCESS_JOB_STATUSES,
    TERMINAL_JOB_STATUSES,
)
from airflow.providers.dx.exceptions import DxJobFailedError, DxLocationValidationError

JobPollResult = Literal["pending", "success", "failed"]


def evaluate_job_status(job: dict[str, Any], job_id: str) -> JobPollResult:
    """Shared job polling logic for sensors and deferrable triggers."""
    status = job.get("status", "")
    if status in SUCCESS_JOB_STATUSES:
        return "success"
    if status in FAILURE_JOB_STATUSES:
        error = job.get("error_message") or status
        raise DxJobFailedError(f"Job {job_id} failed: {error}")
    if status in TERMINAL_JOB_STATUSES:
        raise DxJobFailedError(f"Job {job_id} ended with status {status}")
    return "pending"


def evaluate_location_status(
    status_payload: dict[str, Any],
    location_id: int,
    *,
    fail_on_warning: bool = True,
) -> bool:
    """Return True when location is online; raise or return False otherwise."""
    status = (status_payload.get("status") or "").lower()
    error_message = status_payload.get("error_message")
    if status == LOCATION_STATUS_ONLINE:
        return True
    if status == LOCATION_STATUS_WARNING and fail_on_warning:
        raise DxLocationValidationError(
            f"Location {location_id} validation failed: {error_message}"
        )
    return False
