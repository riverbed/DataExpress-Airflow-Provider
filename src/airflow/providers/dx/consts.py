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

TERMINAL_JOB_STATUSES = frozenset(
    {"Completed", "Failed", "Cancelled", "Stopped"}
)
SUCCESS_JOB_STATUSES = frozenset({"Completed"})
FAILURE_JOB_STATUSES = frozenset({"Failed", "Cancelled", "Stopped"})

LOCATION_STATUS_ONLINE = "online"
LOCATION_STATUS_WARNING = "warning"
LOCATION_STATUS_PENDING = "pending"

# Riverbed Data Express API host (default). Override per env with Extra base_url.
DX_GATEWAY_BASE_URL = "https://api.dx.riverbed.com"

# Optional explicit overrides (testing / break-glass only).
DEFAULT_PROFILES_BASE_URL = ""
DEFAULT_JOBS_BASE_URL = ""

# Legacy gateway layout used /api/profiles and /api/jobs prefixes — strip on normalize.
_LEGACY_API_SUFFIXES = ("/api/profiles", "/api/jobs")


def normalize_api_base(url: str) -> str:
    """Return DX API host root used as base for /api/v1/* paths."""
    normalized = url.strip().rstrip("/")
    if not normalized:
        return ""
    if not normalized.startswith("http"):
        normalized = f"https://{normalized}"
    for suffix in _LEGACY_API_SUFFIXES:
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized
