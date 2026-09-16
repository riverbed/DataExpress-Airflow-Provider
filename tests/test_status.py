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

import pytest

from airflow.providers.dx.exceptions import DxJobFailedError, DxLocationValidationError
from airflow.providers.dx.utils.status import evaluate_job_status, evaluate_location_status


def test_evaluate_job_success():
    assert evaluate_job_status({"status": "Completed"}, "j1") == "success"


def test_evaluate_job_pending():
    assert evaluate_job_status({"status": "Running"}, "j1") == "pending"


def test_evaluate_job_failed():
    with pytest.raises(DxJobFailedError):
        evaluate_job_status({"status": "Failed", "error_message": "boom"}, "j1")


def test_evaluate_location_online():
    assert evaluate_location_status({"status": "online"}, 1) is True


def test_evaluate_location_warning():
    with pytest.raises(DxLocationValidationError):
        evaluate_location_status({"status": "warning", "error_message": "bad creds"}, 1)
