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

import json
from typing import Any
from urllib.parse import urljoin

import requests
from airflow.hooks.base import BaseHook

from airflow.providers.dx.consts import (
    DEFAULT_JOBS_BASE_URL,
    DEFAULT_PROFILES_BASE_URL,
    DX_GATEWAY_BASE_URL,
    normalize_api_base,
)
from airflow.providers.dx.exceptions import DxApiError


class DxHook(BaseHook):
    """
    Hook for Riverbed Data Express APIs (locations, profiles, jobs).

    Auth is a long-lived API token on the Connection, sent on every request as
    ``Authorization: Bearer …``. Set ``extra.api_key``. No login or token-refresh
    calls.

    API requests use ``{base_url}/api/v1/...`` (e.g. ``/api/v1/locations``).
    Default host: ``https://api.dx.riverbed.com``. Override with Extra ``base_url``
    (or ``profiles_base_url`` / ``jobs_base_url`` for split deployments).
    """

    conn_name_attr = "dx_conn_id"
    default_conn_name = "dx_default"
    conn_type = "dx"
    hook_name = "Data Express"

    def __init__(
        self,
        dx_conn_id: str = default_conn_name,
        profiles_base_url: str | None = None,
        jobs_base_url: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.dx_conn_id = dx_conn_id
        self._profiles_base_url = profiles_base_url
        self._jobs_base_url = jobs_base_url

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Connection form behaviour (see Amazon ``AwsBaseHook`` / Kubernetes provider)."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {
                "extra": "Extra JSON",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "api_key": "your-api-key",
                        "base_url": "https://api.dx-dev.riverbed.com",
                    },
                    indent=2,
                ),
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        """Verify API key is set and Jobs API accepts the Bearer token."""
        try:
            self._get_api_key()
        except DxApiError as exc:
            return False, str(exc)

        url = urljoin(f"{self.jobs_base_url}/", "api/v1/jobs/connection-test")
        try:
            response = requests.get(
                url,
                headers=self._auth_headers(),
                timeout=15,
                verify=self.verify_ssl,
            )
        except Exception as exc:
            return False, f"API: {exc}"

        if response.status_code == 401:
            return False, "Auth: HTTP 401"
        return True, f"API key OK (HTTP {response.status_code})"

    @property
    def verify_ssl(self) -> bool:
        extra = self._connection_extra()
        value = extra.get("verify_ssl", True)
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off"}
        return bool(value)

    def get_conn(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self._auth_headers())
        session.verify = self.verify_ssl
        return session

    def _connection_extra(self) -> dict[str, Any]:
        conn = self.get_connection(self.dx_conn_id)
        if not conn.extra:
            return {}
        try:
            return json.loads(conn.extra)
        except json.JSONDecodeError:
            self.log.warning("Invalid JSON in connection extra; ignoring.")
            return {}

    def _default_api_base_url(self) -> str:
        extra = self._connection_extra()
        if candidate := (extra.get("base_url") or "").strip():
            return normalize_api_base(str(candidate))

        conn = self.get_connection(self.dx_conn_id)
        host = (conn.host or "").strip()
        if host:
            if not host.startswith("http"):
                host = f"https://{host}"
            return normalize_api_base(host)

        return normalize_api_base(DX_GATEWAY_BASE_URL)

    def _resolve_api_base(
        self,
        extra_key: str,
        default_constant: str,
        constructor_override: str | None,
    ) -> str:
        if constructor_override:
            return normalize_api_base(constructor_override)
        extra = self._connection_extra()
        if candidate := (extra.get(extra_key) or default_constant or "").strip():
            return normalize_api_base(str(candidate))
        return self._default_api_base_url()

    @property
    def profiles_base_url(self) -> str:
        return self._resolve_api_base(
            "profiles_base_url",
            DEFAULT_PROFILES_BASE_URL,
            self._profiles_base_url,
        )

    @property
    def jobs_base_url(self) -> str:
        return self._resolve_api_base(
            "jobs_base_url",
            DEFAULT_JOBS_BASE_URL,
            self._jobs_base_url,
        )

    def _get_api_key(self) -> str:
        """Long-lived API token sent as Bearer on every request."""
        extra = self._connection_extra()
        key = extra.get("api_key")
        if key and str(key).strip():
            return str(key).strip()
        raise DxApiError(
            "DX connection requires extra.api_key (Bearer token)."
        )

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._get_api_key()}"}

    def _request(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        expected_status: int | tuple[int, ...] = 200,
    ) -> Any:
        if isinstance(expected_status, int):
            expected = (expected_status,)
        else:
            expected = expected_status

        url = urljoin(f"{base_url}/", path.lstrip("/"))
        session = self.get_conn()
        response = session.request(
            method,
            url,
            json=json_body,
            params=params,
            timeout=120,
            verify=self.verify_ssl,
        )

        if response.status_code not in expected:
            raise DxApiError(
                f"{method} {url} failed: {response.status_code} {response.text[:500]}",
                status_code=response.status_code,
                body=response.text,
            )

        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # --- Locations ---

    def create_location(self, location: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            self.profiles_base_url,
            "/api/v1/locations",
            json_body=location,
            expected_status=201,
        )

    def get_location_status(self, location_id: int) -> dict[str, Any]:
        return self._request(
            "GET",
            self.profiles_base_url,
            f"/api/v1/locations/status/{location_id}",
        )

    # --- Profiles ---

    def create_profile(
        self,
        profile_name: str,
        source_location_id: int,
        destination_location_id: int,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            self.profiles_base_url,
            "/api/v1/profiles",
            json_body={
                "profile_name": profile_name,
                "source_location_id": source_location_id,
                "destination_location_id": destination_location_id,
            },
            expected_status=201,
        )

    # --- Jobs ---

    def create_job(
        self,
        profile_id: int,
        job_name: str | None = None,
        agent_only: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"profile_id": profile_id, "agent_only": agent_only}
        if job_name:
            body["job_name"] = job_name
        return self._request(
            "POST",
            self.jobs_base_url,
            "/api/v1/jobs",
            json_body=body,
            expected_status=201,
        )

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            self.jobs_base_url,
            f"/api/v1/jobs/{job_id}",
        )
