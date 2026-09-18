![Riverbed Data Express](docs/images/new_dx_logo.png)

# apache-airflow-providers-dx

Release: **1.0.1**

[Riverbed Data Express (DX)](https://github.com/riverbed/DataExpress-Airflow-Provider)

## Provider package

This is a provider package for the `dx` provider. All classes for this provider package
are in the `airflow.providers.dx` Python package.

Package information and guides: [docs/index.md](docs/index.md).

## Installation

```bash
pip install apache-airflow-providers-dx
```

Requires Python **3.13**.

## Requirements

| PIP package | Version required |
|-------------|------------------|
| `apache-airflow` | `>=3.2.2,<3.3.0` |
| `apache-airflow-providers-dx` | `1.0.1` (this package) |
| `requests` | `>=2.28.0` |

No other Airflow provider packages are required for DX DAGs (only `airflow.providers.dx`).
Run an Airflow **Triggerer** process if you use deferrable DX sensors.

Changelog: [CHANGES.rst](CHANGES.rst).
