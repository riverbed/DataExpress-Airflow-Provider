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


class DxLocationCreateOperator(BaseOperator):
    """
    Create a Data Express location (POST /api/v1/locations).

    Returns the created location dict (including ``id``) via XCom.

    :param location: LocationCreate payload (storage_type, cloud_provider, etc.)
    :param dx_conn_id: Airflow connection id for type ``dx``
    """

    template_fields = ("location",)
    ui_color = "#4a90d9"

    def __init__(
        self,
        *,
        location: dict[str, Any],
        dx_conn_id: str = DxHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.dx_conn_id = dx_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = DxHook(dx_conn_id=self.dx_conn_id)
        result = hook.create_location(self.location)
        self.log.info(
            "Created location id=%s name=%s status=%s",
            result.get("id"),
            result.get("location_name"),
            result.get("status"),
        )
        return result
