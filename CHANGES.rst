Changelog
=========

1.0.1
-----

* README: use absolute GitHub URLs for documentation and changelog links (PyPI-safe).
* Package metadata: ``authors``, ``[project.urls]`` for GitHub repo.

1.0.0
-----

Initial release.

* Requirements: Apache Airflow **3.2.2**, Python **3.13**.
* ``DxHook``: Profiles + Jobs APIs, ``test_connection``, Connection UI field behaviour.
* Default gateway ``https://api.dx.riverbed.com`` (``DX_GATEWAY_BASE_URL``).
  API paths are ``/api/v1/*`` on the host root (no ``/api/profiles`` or ``/api/jobs`` prefix).
  Override per environment with Connection Extra ``base_url`` (or ``profiles_base_url`` /
  ``jobs_base_url``). Legacy prefixed URLs are normalized automatically.
* Auth: ``extra.api_key`` sent as ``Authorization: Bearer`` on every request.
  ``extra.verify_ssl`` for self-signed TLS.
* Operators: location, profile, and job create.
* Sensors: location status and job status (deferrable).
* Triggers: ``DxJobStatusTrigger``, ``DxLocationStatusTrigger``.
* Documentation: ``docs/`` (connection guide, auth, integration guide).
* Examples: ``examples/dags/``, ``examples/connections.example.json``.
