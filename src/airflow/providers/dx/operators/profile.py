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


class DxProfileCreateOperator(BaseOperator):
    """
    Create a transfer profile from existing location IDs.

    :param profile_name: Unique profile name within the tenant
    :param source_location_id: Source location id
    :param destination_location_id: Destination location id
    """

    template_fields = ("profile_name", "source_location_id", "destination_location_id")
    ui_color = "#5b9bd5"

    def __init__(
        self,
        *,
        profile_name: str,
        source_location_id: int | str,
        destination_location_id: int | str,
        dx_conn_id: str = DxHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.profile_name = profile_name
        self.source_location_id = source_location_id
        self.destination_location_id = destination_location_id
        self.dx_conn_id = dx_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        result = hook.create_profile(
            self.profile_name,
            int(self.source_location_id),
            int(self.destination_location_id),
        )
        self.log.info("Created profile id=%s name=%s", result.get("id"), result.get("profile_name"))
        return result
