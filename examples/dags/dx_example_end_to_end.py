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
Minimal DX end-to-end example.

Credentials live in Airflow Connection ``dx_default`` (type ``dx``) — not in this file.
See examples/connections.example.json and docs/connections/dx.md.

Replace SOURCE_LOCATION and DEST_LOCATION with payloads for your storage backend.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.dx.operators.job import DxJobCreateOperator
from airflow.providers.dx.operators.location import DxLocationCreateOperator
from airflow.providers.dx.operators.profile import DxProfileCreateOperator
from airflow.providers.dx.sensors.job import DxJobStatusSensor
from airflow.providers.dx.sensors.location import DxLocationsStatusSensor

# Example OCI S3-compatible location payloads — customize for your environment.
SOURCE_LOCATION = {
    "location_name": "example-source",
    "storage_type": "cloud",
    "cloud_provider": "OCI",
    "region": "us-ashburn-1",
    "access_type": "S3",
    "end_point": "https://your-bucket.compat.objectstorage.region.oci.customer-oci.com",
    "path": "your-bucket/source/path",
    "access_key": "YOUR_OCI_ACCESS_KEY",
    "secret_key": "YOUR_OCI_SECRET_KEY"
}

DEST_LOCATION = {
    "location_name": "example-dest",
    "storage_type": "cloud",
    "cloud_provider": "OCI",
    "region": "us-phoenix-1",
    "access_type": "S3",
    "end_point": "https://your-bucket.compat.objectstorage.region.oci.customer-oci.com",
    "path": "your-bucket/dest/path",
    "access_key": "YOUR_OCI_ACCESS_KEY",
    "secret_key": "YOUR_OCI_SECRET_KEY"
}

with DAG(
    dag_id="dx_example_end_to_end",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["dx", "example"],
    doc_md=__doc__,
    default_args={"execution_timeout": timedelta(hours=6)},
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

    create_profile = DxProfileCreateOperator(
        task_id="create_profile",
        dx_conn_id="dx_default",
        profile_name="example-profile",
        source_location_id="{{ ti.xcom_pull(task_ids='create_source')['id'] }}",
        destination_location_id="{{ ti.xcom_pull(task_ids='create_dest')['id'] }}",
    )

    create_job = DxJobCreateOperator(
        task_id="create_job",
        dx_conn_id="dx_default",
        profile_id="{{ ti.xcom_pull(task_ids='create_profile')['id'] }}",
        job_name="example-transfer-job",
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

    [create_source, create_dest] >> wait_locations >> create_profile >> create_job >> wait_job
