# Configuration reference

Configuration is resolved in the following priority order (highest wins):

1. **Command-line flags** — `--instance-domain`, `--shard-count`, etc.
2. **Config file** — YAML or JSON, default path `./go-jamf-guid-sharder.yaml`
3. **Environment variables** — prefix `JAMF_`, e.g. `JAMF_CLIENT_SECRET`

Use `--config <path>` to specify a non-default config file path.

---

## Authentication

| Config key | Flag | Env var | Type | Required | Description |
|---|---|---|---|---|---|
| `instance_domain` | `--instance-domain` | `JAMF_INSTANCE_DOMAIN` | string | Yes | Your Jamf Pro base URL **including** `https://`, e.g. `https://company.jamfcloud.com` |
| `auth_method` | `--auth-method` | `JAMF_AUTH_METHOD` | string | Yes | `oauth2` or `basic` |
| `client_id` | `--client-id` | `JAMF_CLIENT_ID` | string | When `auth_method=oauth2` | OAuth2 API client ID |
| `client_secret` | `--client-secret` | `JAMF_CLIENT_SECRET` | string | When `auth_method=oauth2` | OAuth2 API client secret |
| `basic_auth_username` | `--username` | `JAMF_BASIC_AUTH_USERNAME` | string | When `auth_method=basic` | Jamf Pro username |
| `basic_auth_password` | `--password` | `JAMF_BASIC_AUTH_PASSWORD` | string | When `auth_method=basic` | Jamf Pro password |

> **Security note:** Prefer environment variables or a config file with restricted permissions (`chmod 600`) over passing secrets as flags. Flags are visible in process listings.

---

## HTTP client tuning

These fields are passed directly to the underlying Jamf Pro SDK client. The defaults work for most environments.

| Config key | Flag | Type | Default | Description |
|---|---|---|---|---|
| `log_level` | `--log-level` | string | `warn` | SDK log verbosity: `debug`, `info`, `warn`, `error`, `fatal` |
| `log_export_path` | `--log-export-path` | string | _(empty)_ | Write SDK logs to this file in addition to stderr |
| `hide_sensitive_data` | `--hide-sensitive-data` | bool | `true` | Redact credentials and tokens from log output |
| `jamf_load_balancer_lock` | `--jamf-load-balancer-lock` | bool | `false` | Lock all requests to a single load balancer node (useful for debugging) |
| `max_retry_attempts` | `--max-retry-attempts` | int | `3` | Maximum retry attempts per request |
| `max_concurrent_requests` | `--max-concurrent-requests` | int | `1` | Maximum concurrent API requests |
| `enable_dynamic_rate_limiting` | `--enable-dynamic-rate-limiting` | bool | `false` | Adapt request rate based on server responses |
| `custom_timeout_seconds` | `--custom-timeout` | int | `60` | Per-request timeout in seconds |
| `token_refresh_buffer_period_seconds` | `--token-refresh-buffer` | int | `300` | Seconds before token expiry to refresh proactively |
| `total_retry_duration_seconds` | `--total-retry-duration` | int | `60` | Maximum total time in seconds to spend retrying a request |
| `follow_redirects` | `--follow-redirects` | bool | `true` | Follow HTTP redirects |
| `max_redirects` | `--max-redirects` | int | `5` | Maximum redirects to follow |
| `enable_concurrency_management` | `--enable-concurrency-management` | bool | `true` | Enable the SDK concurrency manager |
| `mandatory_request_delay_milliseconds` | `--mandatory-request-delay` | int | `0` | Fixed delay between requests in milliseconds |
| `retry_eligiable_requests` | `--retry-eligible-requests` | bool | `true` | Retry eligible failed requests |

---

## Source

| Config key | Flag | Type | Required | Description |
|---|---|---|---|---|
| `source_type` | `--source-type` | string | Yes | Which Jamf Pro data to shard. See table below. |
| `group_id` | `--group-id` | string | When source is `*_group_membership` | Numeric ID of the computer or mobile device group |

**`source_type` values**

| Value | Jamf Pro API | What is fetched |
|---|---|---|
| `computer_inventory` | Pro API | All managed computers |
| `mobile_device_inventory` | Pro API | All managed mobile devices |
| `computer_group_membership` | Classic API | Members of a specific computer group |
| `mobile_device_group_membership` | Classic API | Members of a specific mobile device group |
| `user_accounts` | Classic API | All Jamf Pro user accounts |

> For `computer_group_membership` and `mobile_device_group_membership`, `group_id` must be set to the numeric Jamf Pro group ID (not the name).

---

## Sharding

Exactly one of `shard_count`, `shard_percentages`, or `shard_sizes` must be set. The correct field is determined by the chosen strategy.

| Config key | Flag | Type | Description |
|---|---|---|---|
| `strategy` | `--strategy` | string | Distribution algorithm. See [strategies](strategies.md). One of `round-robin`, `percentage`, `size`, `rendezvous`. |
| `shard_count` | `--shard-count` | int | Number of shards. Required for `round-robin` and `rendezvous`. |
| `shard_percentages` | `--shard-percentages` | `[]int` | Percentages for each shard, must sum to exactly 100. Required for `percentage`. Config file: `[10, 30, 60]`. Flag: `10,30,60`. |
| `shard_sizes` | `--shard-sizes` | `[]int` | Absolute size of each shard. Use `-1` in the final position for "all remaining". Required for `size`. Config file: `[50, 200, -1]`. Flag: `50,200,-1`. |
| `seed` | `--seed` | string | Arbitrary string. When set, IDs are sorted numerically and then deterministically shuffled before distribution. Same seed always produces the same shard assignment. |

---

## Exclusions and reservations

| Config key | Flag | Type | Description |
|---|---|---|---|
| `exclude_ids` | `--exclude-ids` | `[]string` | IDs to remove from all shards before any strategy is applied. Config file: `["1001", "1002"]`. Flag: `1001,1002`. |
| `reserved_ids` | `--reserved-ids` | `map[string][]string` | Pin specific IDs to specific shards. IDs are removed from the general pool first, then appended to their designated shard after the strategy runs. Config file: YAML map (see below). Flag: JSON string. |

**`reserved_ids` in a config file (YAML):**

```yaml
reserved_ids:
  shard_0:
    - "101"
    - "102"
  shard_2:
    - "201"
```

**`reserved_ids` as a flag (JSON string):**

```bash
--reserved-ids '{"shard_0":["101","102"],"shard_2":["201"]}'
```

Shard names must be in the form `shard_N` where N is a zero-based index within the shard count. An ID cannot appear in more than one reserved shard, and cannot appear in both `exclude_ids` and `reserved_ids` simultaneously — the validator will reject either case.

---

## Output

| Config key | Flag | Type | Default | Description |
|---|---|---|---|---|
| `output_format` | `-o` / `--output` | string | `json` | Output format: `json` or `yaml` |
| `output_file` | `--output-file` | string | _(empty)_ | Write output to this file path instead of stdout |

### Output schema

Both JSON and YAML output share the same structure:

```
{
  metadata:
    generated_at              string   — RFC 3339 UTC timestamp of when the run completed
    source_type               string   — source_type used for this run
    group_id                  string   — group_id (omitted if not applicable)
    strategy                  string   — strategy used
    seed                      string   — seed string (empty string if no seed was set)
    total_ids_fetched         int      — raw count fetched from Jamf Pro
    excluded_id_count         int      — number of IDs removed by exclude_ids
    reserved_id_count         int      — number of IDs pinned via reserved_ids
    unreserved_ids_distributed int     — IDs distributed by the strategy
    shard_count               int      — number of shards produced

  shards:
    shard_0: [ "id", ... ]
    shard_1: [ "id", ... ]
    ...
}
```

IDs within each shard are sorted numerically in ascending order.
