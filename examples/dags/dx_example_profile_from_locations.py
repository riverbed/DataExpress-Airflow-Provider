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
Create a DX transfer profile from existing location ids.

Use when source and destination locations already exist and are online.
Set ``EXISTING_SOURCE_LOCATION_ID`` and ``EXISTING_DEST_LOCATION_ID`` in
``dx_example_locations.py``.

Credentials: Connection ``dx_default`` only — see docs/connections/dx.md.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.dx.operators.profile import DxProfileCreateOperator

from dx_example_locations import EXISTING_DEST_LOCATION_ID, EXISTING_SOURCE_LOCATION_ID

with DAG(
    dag_id="dx_example_profile_from_locations",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dx", "example", "profile"],
    doc_md=__doc__,
    default_args={"execution_timeout": timedelta(hours=1)},
) as dag:

    DxProfileCreateOperator(
        task_id="create_profile",
        dx_conn_id="dx_default",
        profile_name="example-profile-from-existing-locations",
        source_location_id=EXISTING_SOURCE_LOCATION_ID,
        destination_location_id=EXISTING_DEST_LOCATION_ID,
    )
