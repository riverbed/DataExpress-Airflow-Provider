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

from airflow.providers.dx.version import __version__

DOCS_BASE_URL = "https://github.com/riverbed/DataExpress-Airflow-Provider/tree/main/docs"


def get_provider_info():
    """Runtime provider metadata (see also ``provider.yaml`` for docs generation)."""
    return {
        "package-name": "apache-airflow-providers-dx",
        "name": "Riverbed Data Express (DX)",
        "description": (
            "Operators, sensors, and deferrable triggers for Data Express "
            "locations, profiles, and data transfer jobs."
        ),
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Riverbed Data Express",
                "external-doc-url": DOCS_BASE_URL,
                "tags": ["service"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Riverbed Data Express",
                "python-modules": ["airflow.providers.dx.hooks.dx"],
            }
        ],
        "operators": [
            {
                "integration-name": "Riverbed Data Express",
                "python-modules": [
                    "airflow.providers.dx.operators.location",
                    "airflow.providers.dx.operators.profile",
                    "airflow.providers.dx.operators.job",
                ],
            }
        ],
        "sensors": [
            {
                "integration-name": "Riverbed Data Express",
                "python-modules": [
                    "airflow.providers.dx.sensors.location",
                    "airflow.providers.dx.sensors.job",
                ],
            }
        ],
        "triggers": [
            {
                "integration-name": "Riverbed Data Express",
                "python-modules": [
                    "airflow.providers.dx.triggers.job",
                    "airflow.providers.dx.triggers.location",
                ],
            }
        ],
        "connection-types": [
            {
                "connection-type": "dx",
                "hook-class-name": "airflow.providers.dx.hooks.dx.DxHook",
                "hook-name": "Data Express",
            }
        ],
    }
