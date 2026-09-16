# Examples

Copy these into your Airflow `dags/` folder after installing the provider.

| File | DAG id | Description |
|------|--------|-------------|
| `dags/dx_example_locations_only.py` | `dx_example_locations_only` | Create source + dest locations, wait until online |
| `dags/dx_example_locations_and_profile.py` | `dx_example_locations_and_profile` | Locations → wait online → create profile |
| `dags/dx_example_profile_from_locations.py` | `dx_example_profile_from_locations` | Create profile from existing location ids |
| `dags/dx_example_profile_job_from_locations.py` | `dx_example_profile_job_from_locations` | Profile + job from existing location ids |
| `dags/dx_example_job_from_profile.py` | `dx_example_job_from_profile` | Job from existing profile id (manual trigger) |
| `dags/dx_example_scheduled_job_from_profile.py` | `dx_example_scheduled_job_from_profile` | Scheduled daily job from existing profile id |
| `dags/dx_example_end_to_end.py` | `dx_example_end_to_end` | Full pipeline: locations → profile → job |
| `dags/dx_example_locations.py` | *(not a DAG)* | Shared location payloads and placeholder ids |
| `connections.example.json` | — | Connection template for `airflow connections import` |

**Auth:** configure Connection `dx_default` first — see [docs/connections/dx.md](../docs/connections/dx.md).

No credentials belong in DAG files — only `dx_conn_id="dx_default"`. Bucket credentials go in location payloads in `dx_example_locations.py` (or inline in `dx_example_end_to_end.py`).
