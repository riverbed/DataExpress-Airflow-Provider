# Auth

DX authenticates with a **long-lived API token**. `DxHook` reads it from the Airflow
Connection and sends it on **every** Profiles/Jobs request as
`Authorization: Bearer <token>`. No login, refresh, or sho-auth calls.

```
Connection extra.api_key
  → Authorization: Bearer <api_key>
  → Profiles / Jobs API calls
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
| Airflow Connection UI | Local dev |
| `airflow connections import` | Dev/staging templates |
| `AIRFLOW_CONN_DX_DEFAULT` env | Docker / K8s |
| Secrets backend (Vault, AWS SM) | Production |

## Related

- [DX Connection](connections/dx.md)
- [Integration guide](integration-guide.md)
