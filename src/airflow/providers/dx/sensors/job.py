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

from datetime import timedelta
from typing import Any

from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from airflow.providers.dx.exceptions import DxJobFailedError
from airflow.providers.dx.hooks.dx import DxHook
from airflow.providers.dx.triggers.job import DxJobStatusTrigger
from airflow.providers.dx.utils.status import evaluate_job_status


class DxJobStatusSensor(BaseSensorOperator):
    """
    Wait until a transfer job reaches a terminal status.

    Succeeds on ``Completed``. Fails on ``Failed``, ``Cancelled``, ``Stopped``.

    Supports deferrable mode (default follows ``[operators] default_deferrable``),
    matching ``HttpSensor`` and ``KubernetesJobTrigger`` patterns.

    :param job_id: Job id returned from DxJobCreateOperator
    :param deferrable: Run on the triggerer instead of worker slot while waiting
    """

    template_fields = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        dx_conn_id: str = DxHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.dx_conn_id = dx_conn_id
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        job = hook.get_job(self.job_id)
        self.log.info(
            "Job %s status=%s progress=%s",
            self.job_id,
            job.get("status"),
            job.get("progress"),
        )
        return evaluate_job_status(job, self.job_id) == "success"

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context)
        if self.poke(context):
            return None
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=DxJobStatusTrigger(
                job_id=self.job_id,
                dx_conn_id=self.dx_conn_id,
                poll_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if not event:
            raise DxJobFailedError(f"No event returned for job {self.job_id}")
        if event.get("status") == "error":
            raise DxJobFailedError(event.get("message", "Job monitoring failed"))
        self.log.info("Job %s completed: %s", self.job_id, event.get("job", {}).get("status"))
