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
Scheduled DX transfer job using an existing profile.

Runs daily at 02:00 UTC. Each run gets a unique job name via ``ts_nodash``.
Set ``EXISTING_PROFILE_ID`` in ``dx_example_locations.py``.

Credentials: Connection ``dx_default`` only — see docs/connections/dx.md.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.dx.operators.job import DxJobCreateOperator
from airflow.providers.dx.sensors.job import DxJobStatusSensor

from dx_example_locations import EXISTING_PROFILE_ID

with DAG(
    dag_id="dx_example_scheduled_job_from_profile",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["dx", "example", "scheduled", "job"],
    doc_md=__doc__,
    default_args={"execution_timeout": timedelta(hours=6)},
) as dag:

    create_job = DxJobCreateOperator(
        task_id="create_job",
        dx_conn_id="dx_default",
        profile_id=EXISTING_PROFILE_ID,
        job_name="scheduled-transfer-{{ ts_nodash }}",
        agent_only=False,
    )

    wait_job = DxJobStatusSensor(
        task_id="wait_for_job",
        dx_conn_id="dx_default",
        job_id="{{ ti.xcom_pull(task_ids='create_job')['job_id'] }}",
        poke_interval=30,
        timeout=60 * 60 * 4,
        mode="reschedule",
    )

    create_job >> wait_job
