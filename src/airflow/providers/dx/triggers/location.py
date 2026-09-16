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
from airflow.providers.dx.utils.status import evaluate_location_status


class DxLocationStatusTrigger(BaseTrigger):
    """Deferrable trigger that polls location status until online."""

    def __init__(
        self,
        location_id: int,
        dx_conn_id: str = DxHook.default_conn_name,
        poll_interval: float = 20.0,
        fail_on_warning: bool = True,
    ) -> None:
        super().__init__()
        self.location_id = location_id
        self.dx_conn_id = dx_conn_id
        self.poll_interval = poll_interval
        self.fail_on_warning = fail_on_warning

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.dx.triggers.location.DxLocationStatusTrigger",
            {
                "location_id": self.location_id,
                "dx_conn_id": self.dx_conn_id,
                "poll_interval": self.poll_interval,
                "fail_on_warning": self.fail_on_warning,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        while True:
            payload = await asyncio.to_thread(hook.get_location_status, self.location_id)
            status = payload.get("status")
            self.log.info("Location %s status=%s", self.location_id, status)
            try:
                if evaluate_location_status(
                    payload,
                    self.location_id,
                    fail_on_warning=self.fail_on_warning,
                ):
                    yield TriggerEvent({"status": "success", "location_status": payload})
                    return
            except Exception as exc:
                yield TriggerEvent({"status": "error", "message": str(exc)})
                return
            await asyncio.sleep(self.poll_interval)
