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

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow.providers.dx.hooks.dx import DxHook


class DxJobCreateOperator(BaseOperator):
    """
    Start a data transfer job (POST /api/v1/jobs).

    :param profile_id: Profile id from Profiles API
    :param job_name: Optional job name (auto-generated if omitted)
    :param agent_only: Request agent-only deployment when supported
    """

    template_fields = ("job_name", "profile_id")
    ui_color = "#1f4e79"

    def __init__(
        self,
        *,
        profile_id: int | str,
        job_name: str | None = None,
        agent_only: bool = False,
        dx_conn_id: str = DxHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.profile_id = profile_id
        self.job_name = job_name
        self.agent_only = agent_only
        self.dx_conn_id = dx_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        profile_id = int(self.profile_id)
        result = hook.create_job(
            profile_id=profile_id,
            job_name=self.job_name,
            agent_only=self.agent_only,
        )
        self.log.info(
            "Created job job_id=%s status=%s profile_id=%s",
            result.get("job_id"),
            result.get("status"),
            result.get("profile_id"),
        )
        return result
