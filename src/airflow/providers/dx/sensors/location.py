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
from typing import Any, Iterable

from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from airflow.providers.dx.exceptions import DxLocationValidationError
from airflow.providers.dx.hooks.dx import DxHook
from airflow.providers.dx.triggers.location import DxLocationStatusTrigger
from airflow.providers.dx.utils.status import evaluate_location_status


class DxLocationStatusSensor(BaseSensorOperator):
    """
    Wait until a location reaches ``online`` status.

    :param location_id: Location id to monitor
    :param fail_on_warning: Fail when status is ``warning``
    :param deferrable: Use triggerer while waiting (recommended for long validation)
    """

    template_fields = ("location_id",)

    def __init__(
        self,
        *,
        location_id: int | str,
        fail_on_warning: bool = True,
        dx_conn_id: str = DxHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location_id = location_id
        self.fail_on_warning = fail_on_warning
        self.dx_conn_id = dx_conn_id
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        location_id = int(self.location_id)
        payload = hook.get_location_status(location_id)
        self.log.info(
            "Location %s status=%s error=%s",
            location_id,
            payload.get("status"),
            payload.get("error_message"),
        )
        return evaluate_location_status(
            payload, location_id, fail_on_warning=self.fail_on_warning
        )

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context)
        if self.poke(context):
            return None
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=DxLocationStatusTrigger(
                location_id=int(self.location_id),
                dx_conn_id=self.dx_conn_id,
                poll_interval=self.poke_interval,
                fail_on_warning=self.fail_on_warning,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if not event:
            raise DxLocationValidationError(f"No event for location {self.location_id}")
        if event.get("status") == "error":
            raise DxLocationValidationError(event.get("message", "Location validation failed"))
        self.log.info("Location %s is online", self.location_id)


class DxLocationsStatusSensor(BaseSensorOperator):
    """Wait until all given location ids are ``online`` (sequential poke)."""

    template_fields = ("location_ids",)

    def __init__(
        self,
        *,
        location_ids: Iterable[int | str],
        fail_on_warning: bool = True,
        dx_conn_id: str = DxHook.default_conn_name,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location_ids = list(location_ids)
        self.fail_on_warning = fail_on_warning
        self.dx_conn_id = dx_conn_id
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        for raw_id in self.location_ids:
            sensor = DxLocationStatusSensor(
                task_id=f"_check_location_{raw_id}",
                location_id=raw_id,
                fail_on_warning=self.fail_on_warning,
                dx_conn_id=self.dx_conn_id,
                deferrable=False,
            )
            if not sensor.poke(context):
                return False
        return True
