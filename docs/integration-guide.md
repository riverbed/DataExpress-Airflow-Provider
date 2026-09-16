# Integration guide

How to integrate Riverbed Data Express with Apache Airflow using `apache-airflow-providers-dx`.

## Architecture

```text
Airflow DAG
  → DxOperator / DxSensor (dx_conn_id="dx_default")
    → DxHook
      → Airflow Connection (type=dx)  ← credentials here
        → Profiles API  (locations, profiles)
        → Jobs API      (transfer jobs)
```

Airflow does not run data transfers. It calls DX REST APIs to create resources and poll status.

Typical pipeline (one-time setup):

```text
create source location
  → create dest location
  → wait locations online
  → create profile
  → create job
  → wait job completed
```

Run once when locations and profile do not exist. Save the profile `id` for recurring runs.

### Repeated job from existing profile

When the profile (and locations) already exist — created manually, in the DX UI, or by a prior DAG run —
use a shorter pipeline on a schedule:

```text
(existing profile_id)
  → create job
  → wait job completed
```

Each scheduled run starts a **new** DX job against the same profile. Set Airflow `schedule` on the DAG
(e.g. `schedule="0 2 * * *"`). Pass `profile_id` as a constant, Airflow Variable, or DAG param.
Use a unique `job_name` per run (e.g. `transfer-{{ ts_nodash }}`).

```python
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.dx.operators.job import DxJobCreateOperator
from airflow.providers.dx.sensors.job import DxJobStatusSensor

PROFILE_ID = int(Variable.get("dx_profile_id", default_var="42"))

with DAG(
    dag_id="dx_recurring_transfer",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["dx"],
    default_args={"execution_timeout": timedelta(hours=6)},
) as dag:
    create_job = DxJobCreateOperator(
        task_id="create_job",
        dx_conn_id="dx_default",
        profile_id=PROFILE_ID,
        job_name="transfer-{{ ts_nodash }}",
    )
    wait_job = DxJobStatusSensor(
        task_id="wait_for_job",
        dx_conn_id="dx_default",
        job_id="{{ ti.xcom_pull(task_ids='create_job')['job_id'] }}",
        poke_interval=30,
        timeout=60 * 60 * 4,
        mode="reschedule",
    )
    create_job >> wait_job
```

Common pattern: **two DAGs** — setup (`dx_example_end_to_end.py`, trigger once) and recurring
(`dx_example_scheduled_job_from_profile.py` or `dx_example_job_from_profile.py` with `schedule` set).
Deferrable sensors require Triggerer.

## Requirements

### Python packages (tested)

| Package | Version | Notes |
|---------|---------|-------|
| `apache-airflow` | **3.2.2+** (<3.3) | Core platform |
| `apache-airflow-providers-dx` | **1.0.0** | **Only additional provider package required** |
| Python | **3.13** | Runtime |
| `requests` | ≥2.28 | Pulled in by this provider |

DX DAGs import `airflow.providers.dx` only — no other Airflow provider packages
(Jira, Amazon, HTTP, etc.) are required.

### Airflow components

| Component | Required? | Notes |
|-----------|-----------|-------|
| Scheduler + workers | Yes | Execute operators |
| Triggerer | If deferrable sensors | `DxJobStatusSensor` / `DxLocationsStatusSensor` with `deferrable=True` |
| Connection `dx_default` | Yes | Type `dx`, `extra.api_key` |

Network: outbound HTTPS to Riverbed DX SaaS.

## Installation

```bash
pip install apache-airflow-providers-dx
```

Verify:

```bash
airflow providers list | grep dx
```

## Step 1 — Configure Connection

Create connection **`dx_default`** (type **`dx`**). See [DX Connection](connections/dx.md).

Gateway URL is built into the provider. Users only supply credentials.

Recommended — API key:

```json
{
  "api_key": "YOUR_DX_API_KEY"
}
```

Import from file:

```bash
airflow connections import --overwrite examples/connections.example.json
```

## Step 2 — Test Connection

```bash
airflow connections test dx_default
```

## Step 3 — Add a DAG

Every operator uses **`dx_conn_id`** only — credentials stay on Connection.

### Create location

```python
DxLocationCreateOperator(
    task_id="create_source",
    dx_conn_id="dx_default",
    location={
        "location_name": "my-source",
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
    },
)
```

### Create profile

Standard Airflow pattern: upstream operators return dicts via XCom; downstream
operators pull IDs with Jinja on templated fields (`template_fields`).

Task order must be set: `create_source >> create_profile`.

```python
DxProfileCreateOperator(
    task_id="create_profile",
    dx_conn_id="dx_default",
    profile_name="my-profile",
    source_location_id="{{ ti.xcom_pull(task_ids='create_source')['id'] }}",
    destination_location_id="{{ ti.xcom_pull(task_ids='create_dest')['id'] }}",
)
```

For locations that already exist, pass integer IDs directly instead of XCom templates.

### Create job

```python
DxJobCreateOperator(
    task_id="create_job",
    dx_conn_id="dx_default",
    profile_id="{{ ti.xcom_pull(task_ids='create_profile')['id'] }}",
    job_name="my-transfer",
)
```

Full pipeline: [`examples/dags/dx_example_end_to_end.py`](../examples/dags/dx_example_end_to_end.py)

### Example DAGs

| File | Use case |
|------|----------|
| `dx_example_locations_only.py` | Create locations, wait until online |
| `dx_example_locations_and_profile.py` | Locations → wait → profile |
| `dx_example_profile_from_locations.py` | Profile from existing location ids |
| `dx_example_profile_job_from_locations.py` | Profile + job from existing location ids |
| `dx_example_job_from_profile.py` | Job from existing profile (manual) |
| `dx_example_scheduled_job_from_profile.py` | Scheduled job from existing profile |
| `dx_example_end_to_end.py` | Full pipeline: locations → profile → job |
| `dx_example_locations.py` | Shared payloads/ids (not a DAG) |

## Use-case recipes

| Goal | DAG pattern |
|------|-------------|
| Full new transfer | 2× `DxLocationCreateOperator` → `DxLocationsStatusSensor` → `DxProfileCreateOperator` → `DxJobCreateOperator` → `DxJobStatusSensor` |
| Locations only | 2× `DxLocationCreateOperator` → `DxLocationsStatusSensor` |
| Job from existing profile | `DxJobCreateOperator` → `DxJobStatusSensor` |
| Scheduled / repeated transfer | Existing `profile_id` → `DxJobCreateOperator` → `DxJobStatusSensor`; DAG `schedule` set |
| Locations + profile | 2× `DxLocationCreateOperator` → `DxLocationsStatusSensor` → `DxProfileCreateOperator` |
| Profile from existing locations | `DxProfileCreateOperator` only |
| Profile + job from existing locations | `DxProfileCreateOperator` → `DxJobCreateOperator` → `DxJobStatusSensor` |

## Multi-environment

Use different connection ids — same DAG code:

| Environment | Connection id |
|-------------|---------------|
| Dev | `dx_dev` |
| Staging | `dx_staging` |
| Prod | `dx_prod` |

Pass `dx_conn_id="dx_prod"` on operators or use Airflow Variables for the conn id name (not the secret).

## Storage credentials (OCI/S3)

Bucket access keys belong in **location payload** or external config loaded by a `@task` — not in the
Airflow Connection. The DX Connection authenticates **to DX APIs** only.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `CERTIFICATE_VERIFY_FAILED` | `extra.verify_ssl: false` (staging) or install CA |
| `Auth: HTTP 401` | Wrong or revoked `api_key`; check Connection |
| Duplicate location on rerun | Use unique paths or existing location ids |

## Related

- [DX Connection](connections/dx.md)
- [Auth](auth-modes.md)
- [Example DAGs](../examples/dags/)
