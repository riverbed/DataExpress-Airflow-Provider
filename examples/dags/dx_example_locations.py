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

"""Shared location payloads and ids for DX example DAGs (not a DAG itself)."""

from __future__ import annotations

# Example OCI S3-compatible location payloads — customize for your environment.
SOURCE_LOCATION = {
    "location_name": "example-source",
    "storage_type": "cloud",
    "cloud_provider": "OCI",
    "region": "us-ashburn-1",
    "access_type": "S3",
    "end_point": "https://your-bucket.compat.objectstorage.region.oci.customer-oci.com",
    "path": "your-bucket/source/path",
    "credentials": {
        "access_key": "YOUR_OCI_ACCESS_KEY",
        "secret_key": "YOUR_OCI_SECRET_KEY",
    },
}

DEST_LOCATION = {
    "location_name": "example-dest",
    "storage_type": "cloud",
    "cloud_provider": "OCI",
    "region": "us-phoenix-1",
    "access_type": "S3",
    "end_point": "https://your-bucket.compat.objectstorage.region.oci.customer-oci.com",
    "path": "your-bucket/dest/path",
    "credentials": {
        "access_key": "YOUR_OCI_ACCESS_KEY",
        "secret_key": "YOUR_OCI_SECRET_KEY",
    },
}

# Replace with ids from your DX tenant when using "existing resource" examples.
EXISTING_SOURCE_LOCATION_ID = 1
EXISTING_DEST_LOCATION_ID = 2
EXISTING_PROFILE_ID = 1
