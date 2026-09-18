# DX Connection

The **dx** connection type enables orchestration of Riverbed Data Express (Profiles and Jobs APIs).

## Default Connection IDs

`DxHook` and all DX operators/sensors use parameter **`dx_conn_id`**. Default value: **`dx_default`**.

## DX gateway

Default SaaS gateway: **`https://api.dx.riverbed.com`**.

API calls use **`{base_url}/api/v1/...`** (e.g. `POST …/api/v1/locations`, `GET …/api/v1/jobs/{id}`).
No `/api/profiles` or `/api/jobs` prefix on the host.

## Configuring the Connection

### Connection Type

Set **Connection Type** to **`dx`**.

### Host

Not required. Gateway URL is built into the provider.

### Login / Password / Host

Not used. Hidden in the Connection UI. Token lives in Extra `api_key` only.

### Port

Not used — leave default.

### Extra

JSON object. Common fields:

| Extra field | Required | Description |
|-------------|----------|-------------|
| `api_key` | Yes | Bearer token sent on every DX request |

See [Auth](../auth-modes.md).

## Example — API key

**Airflow UI:** Admin → Connections → Add

| Field | Value |
|-------|-------|
| Connection Id | `dx_default` |
| Connection Type | `dx` |
| Host | *(empty)* |
| Login | *(empty)* |
| Password | *(empty)* |
| Extra | see JSON below |

```json
{
  "api_key": "YOUR_DX_API_KEY"
}
```

`verify_ssl` optional — omit for default `true`; set `false` only for self-signed TLS.

**JSON import file:** [`examples/connections.example.json`](../../examples/connections.example.json)

**Environment variable:**

```bash
export AIRFLOW_CONN_DX_DEFAULT='dx://?extra__api_key=YOUR_DX_API_KEY'
```

## Test the Connection

```bash
airflow connections test dx_default
```

Expected output:

- `API key OK (Jobs API HTTP …)` — any status except `401` means the token was accepted

## Security

- Use Airflow [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) or `AIRFLOW_CONN_*` env vars in production.

## Related

- [Auth](../auth-modes.md)
- [Integration guide](../integration-guide.md)
