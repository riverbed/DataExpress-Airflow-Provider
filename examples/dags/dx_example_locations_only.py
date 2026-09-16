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

"""
Create a source and destination DX location and wait until both are online.

Credentials: Connection ``dx_default`` only — see docs/connections/dx.md.
Customize payloads in ``dx_example_locations.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.dx.operators.location import DxLocationCreateOperator
from airflow.providers.dx.sensors.location import DxLocationsStatusSensor

from dx_example_locations import DEST_LOCATION, SOURCE_LOCATION

with DAG(
    dag_id="dx_example_locations_only",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["dx", "example", "locations"],
    doc_md=__doc__,
    default_args={"execution_timeout": timedelta(hours=2)},
) as dag:

    create_source = DxLocationCreateOperator(
        task_id="create_source",
        dx_conn_id="dx_default",
        location=SOURCE_LOCATION,
    )
    create_dest = DxLocationCreateOperator(
        task_id="create_dest",
        dx_conn_id="dx_default",
        location=DEST_LOCATION,
    )

    wait_locations = DxLocationsStatusSensor(
        task_id="wait_locations_online",
        dx_conn_id="dx_default",
        location_ids=[
            "{{ ti.xcom_pull(task_ids='create_source')['id'] }}",
            "{{ ti.xcom_pull(task_ids='create_dest')['id'] }}",
        ],
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
    )

    [create_source, create_dest] >> wait_locations
