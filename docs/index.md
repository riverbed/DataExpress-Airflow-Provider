# apache-airflow-providers-dx documentation

Documentation for the **Riverbed Data Express (DX)** Airflow provider.

> Source: [riverbed/DataExpress-Airflow-Provider](https://github.com/riverbed/DataExpress-Airflow-Provider)

## Guides

| Guide | Description |
|-------|-------------|
| [DX Connection](connections/dx.md) | Connection fields, default conn id, configuration (Jira-style) |
| [Auth](auth-modes.md) | API token as Bearer on every request |
| [Integration guide](integration-guide.md) | Install, test connection, example DAGs, use cases |

## Package

| Package | Tested version |
|---------|----------------|
| PyPI name | `apache-airflow-providers-dx` |
| Provider version | **1.0.0** |
| Python module | `airflow.providers.dx` |
| Connection type | `dx` |
| Default connection id | `dx_default` |
| Apache Airflow | **3.2.2** |
| Python | **3.13** |

**Prerequisites:** install `apache-airflow-providers-dx` on Airflow 3.2.2+. No other
Airflow provider packages are required for DX DAGs. Run an Airflow **Triggerer** if you
use deferrable DX sensors (`deferrable=True`).

## Install

```bash
pip install apache-airflow-providers-dx
```

From source (until PyPI publish):

```bash
pip install /path/to/apache-airflow-providers-dx
```

## Quick start

1. Create Airflow Connection `dx_default` (type **dx**) — see [DX Connection](connections/dx.md).
2. Test: `airflow connections test dx_default`
3. Copy an example DAG from [`examples/dags/`](../examples/dags/).
4. Point `dx_conn_id="dx_default"` on operators — **no credentials in DAG code**.

## Components

| Type | Classes |
|------|---------|
| Hook | `DxHook` |
| Operators | `DxLocationCreateOperator`, `DxProfileCreateOperator`, `DxJobCreateOperator` |
| Sensors | `DxLocationStatusSensor`, `DxLocationsStatusSensor`, `DxJobStatusSensor` |
| Triggers | `DxLocationStatusTrigger`, `DxJobStatusTrigger` |

## Changelog

See [CHANGES.rst](../CHANGES.rst).

## Examples

Copy DAGs from [`examples/dags/`](../examples/dags/) into your Airflow `dags/` folder.
This package does not include a Docker Compose stack.
