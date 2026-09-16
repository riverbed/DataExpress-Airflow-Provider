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

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.dx.consts import normalize_api_base
from airflow.providers.dx.exceptions import DxApiError
from airflow.providers.dx.hooks.dx import DxHook

EXAMPLE_GATEWAY = "https://api.dx.example.com"


@pytest.fixture
def hook():
    with patch.object(DxHook, "get_connection") as mock_conn:
        conn = MagicMock()
        conn.host = None
        conn.extra = json.dumps(
            {
                "api_key": "test-token",
                "base_url": EXAMPLE_GATEWAY,
            }
        )
        mock_conn.return_value = conn
        yield DxHook(dx_conn_id="dx_default")


def test_normalize_api_base_strips_legacy_profiles_suffix():
    assert (
        normalize_api_base("https://api.dx.example.com/api/profiles")
        == EXAMPLE_GATEWAY
    )


def test_api_base_urls_from_extra_base_url(hook):
    assert hook.profiles_base_url == EXAMPLE_GATEWAY
    assert hook.jobs_base_url == EXAMPLE_GATEWAY


def test_api_base_url_from_connection_host():
    with patch.object(DxHook, "get_connection") as mock_conn:
        conn = MagicMock()
        conn.host = EXAMPLE_GATEWAY
        conn.extra = json.dumps({"api_key": "test-token"})
        mock_conn.return_value = conn
        hook = DxHook(dx_conn_id="dx_default")

    assert hook.profiles_base_url == EXAMPLE_GATEWAY
    assert hook.jobs_base_url == EXAMPLE_GATEWAY


@patch("airflow.providers.dx.hooks.dx.requests.Session")
def test_create_job(mock_session_cls, hook):
    session = MagicMock()
    mock_session_cls.return_value = session
    response = MagicMock()
    response.status_code = 201
    response.content = b'{"job_id": "jabc1234567", "status": "Pending"}'
    response.json.return_value = {
        "job_id": "jabc1234567",
        "status": "Pending",
        "profile_id": 1,
        "job_name": "Test",
    }
    session.request.return_value = response

    result = hook.create_job(profile_id=1, job_name="Test")
    assert result["job_id"] == "jabc1234567"
    call_kwargs = session.request.call_args[1]
    assert call_kwargs["json"]["profile_id"] == 1
    assert session.request.call_args[0][1] == f"{EXAMPLE_GATEWAY}/api/v1/jobs"


def test_verify_ssl_default_true(hook):
    assert hook.verify_ssl is True


def test_verify_ssl_false_from_extra(hook):
    hook.get_connection.return_value.extra = json.dumps(
        {
            "base_url": EXAMPLE_GATEWAY,
            "verify_ssl": False,
        }
    )
    assert hook.verify_ssl is False


def test_api_key_used_as_bearer(hook):
    headers = hook._auth_headers()
    assert headers == {"Authorization": "Bearer test-token"}


def test_missing_api_key_raises(hook):
    hook.get_connection.return_value.extra = json.dumps(
        {"base_url": EXAMPLE_GATEWAY}
    )

    with pytest.raises(DxApiError, match="extra.api_key"):
        hook._get_api_key()


@patch("airflow.providers.dx.hooks.dx.requests.Session")
def test_create_job_sends_bearer(mock_session_cls, hook):
    session = MagicMock()
    mock_session_cls.return_value = session
    response = MagicMock()
    response.status_code = 201
    response.content = b'{"job_id": "j1"}'
    response.json.return_value = {"job_id": "j1"}
    session.request.return_value = response

    hook.create_job(profile_id=1, job_name="Test")

    session.headers.update.assert_called()
    headers = session.headers.update.call_args[0][0]
    assert headers["Authorization"] == "Bearer test-token"


def test_verify_ssl_default_true_when_omitted():
    with patch.object(DxHook, "get_connection") as mock_conn:
        conn = MagicMock()
        conn.host = None
        conn.extra = json.dumps({"api_key": "dx-api-key-123"})
        mock_conn.return_value = conn
        assert DxHook(dx_conn_id="dx_default").verify_ssl is True


def test_default_api_base_from_builtin_gateway():
    from airflow.providers.dx.consts import DX_GATEWAY_BASE_URL

    with patch.object(DxHook, "get_connection") as mock_conn:
        conn = MagicMock()
        conn.host = None
        conn.extra = json.dumps({"api_key": "dx-api-key-123"})
        mock_conn.return_value = conn
        hook = DxHook(dx_conn_id="dx_default")

    assert hook.profiles_base_url == DX_GATEWAY_BASE_URL
    assert hook.jobs_base_url == DX_GATEWAY_BASE_URL
