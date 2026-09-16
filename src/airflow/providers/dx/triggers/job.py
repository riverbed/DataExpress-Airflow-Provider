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

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow.providers.dx.hooks.dx import DxHook
from airflow.providers.dx.utils.status import evaluate_job_status


class DxJobStatusTrigger(BaseTrigger):
    """
    Deferrable trigger that polls the Jobs API until a terminal state.

    Mirrors the polling pattern used by ``HttpSensorTrigger`` and
    ``KubernetesJobTrigger`` in official providers.
    """

    def __init__(
        self,
        job_id: str,
        dx_conn_id: str = DxHook.default_conn_name,
        poll_interval: float = 30.0,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        self.dx_conn_id = dx_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.dx.triggers.job.DxJobStatusTrigger",
            {
                "job_id": self.job_id,
                "dx_conn_id": self.dx_conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        while True:
            job = await asyncio.to_thread(hook.get_job, self.job_id)
            status = job.get("status", "")
            progress = job.get("progress")
            self.log.info("Job %s status=%s progress=%s", self.job_id, status, progress)
            try:
                result = evaluate_job_status(job, self.job_id)
            except Exception as exc:
                yield TriggerEvent({"status": "error", "message": str(exc), "job": job})
                return
            if result == "success":
                yield TriggerEvent({"status": "success", "job": job})
                return
            await asyncio.sleep(self.poll_interval)
