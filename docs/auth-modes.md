# Auth

DX authenticates with a **long-lived API token**. `DxHook` reads it from the Airflow
Connection and sends it on **every API call** as
`Authorization: Bearer <token>`.

```
Connection extra.api_key
  → Authorization: Bearer <api_key>
  → every API call
```

## Connection setup

| Field | Value |
|-------|-------|
| Extra `api_key` | API token (required) |

Login, password, and host are unused (hidden in Connection UI).

Never put tokens in DAG `params`, Variables, or source control.

## Storing credentials

| Method | Use |
|--------|-----|
| Airflow Connection UI | Manual setup |
| `airflow connections import` | JSON connection templates |
| `AIRFLOW_CONN_DX_DEFAULT` env | Docker / Kubernetes |
| Secrets backend (Vault, AWS SM) | Recommended for production |

## Related

- [DX Connection](connections/dx.md)
- [Integration guide](integration-guide.md)
