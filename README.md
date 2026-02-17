# go-jamf-guid-sharder

A CLI tool that retrieves managed device and user IDs from Jamf Pro and distributes them into configurable shards for progressive rollouts and phased deployments.

[![Go Test](https://github.com/deploymenttheory/go-jamf-guid-sharder/actions/workflows/go-test.yml/badge.svg)](https://github.com/deploymenttheory/go-jamf-guid-sharder/actions/workflows/go-test.yml)
[![Go Lint](https://github.com/deploymenttheory/go-jamf-guid-sharder/actions/workflows/go-lint.yml/badge.svg)](https://github.com/deploymenttheory/go-jamf-guid-sharder/actions/workflows/go-lint.yml)
[![Release](https://github.com/deploymenttheory/go-jamf-guid-sharder/actions/workflows/release.yml/badge.svg)](https://github.com/deploymenttheory/go-jamf-guid-sharder/actions/workflows/release.yml)
[![License](https://img.shields.io/github/license/deploymenttheory/go-jamf-guid-sharder)](LICENSE)

## What it does

`go-jamf-guid-sharder` connects to Jamf Pro, fetches a set of managed device or user IDs, and splits them into named shards using one of four algorithms. The output is JSON or YAML — ready to pipe into a deployment tool, Terraform data source, or further automation.

```
Jamf Pro API  →  fetch IDs  →  exclude / reserve  →  shard  →  JSON / YAML
```

**Supported sources**

| `source_type` | API used | Notes |
|---|---|---|
| `computer_inventory` | Pro API | Managed computers only |
| `mobile_device_inventory` | Pro API | Managed mobile devices only |
| `computer_group_membership` | Classic API | Requires `--group-id` |
| `mobile_device_group_membership` | Classic API | Requires `--group-id` |
| `user_accounts` | Classic API | All Jamf Pro user accounts |

**Supported strategies**

| Strategy | Description |
|---|---|
| `round-robin` | Equal distribution ±1, optionally seeded |
| `percentage` | Proportional split by explicit percentages summing to 100 |
| `size` | Absolute shard sizes; use `-1` as final element for remainder |
| `rendezvous` | Highest Random Weight (HRW) consistent hashing — minimal movement when shard count changes |

## Quick start

```bash
# Download the latest release binary (macOS arm64 example)
curl -L https://github.com/deploymenttheory/go-jamf-guid-sharder/releases/latest/download/go-jamf-guid-sharder_latest_darwin_arm64.tar.gz | tar xz

# Run a round-robin split of all managed computers into 3 shards
./go-jamf-guid-sharder shard \
  --instance-domain company.jamfcloud.com \
  --auth-method oauth2 \
  --client-id <id> --client-secret <secret> \
  --source-type computer_inventory \
  --strategy round-robin --shard-count 3
```

Output:

```json
{
  "metadata": {
    "generated_at": "2024-11-01T09:15:42Z",
    "source_type": "computer_inventory",
    "strategy": "round-robin",
    "seed": "",
    "total_ids_fetched": 1200,
    "excluded_id_count": 0,
    "reserved_id_count": 0,
    "unreserved_ids_distributed": 1200,
    "shard_count": 3
  },
  "shards": {
    "shard_0": ["1", "4", "7"],
    "shard_1": ["2", "5", "8"],
    "shard_2": ["3", "6", "9"]
  }
}
```

## Documentation

| Topic | Description |
|---|---|
| [Getting started](docs/getting-started.md) | Installation, first run, config file setup |
| [Configuration reference](docs/configuration.md) | Every field and flag explained |
| [Sharding strategies](docs/strategies.md) | How each algorithm works, when to use it |
| [Examples](docs/examples.md) | Real-world patterns for common rollout scenarios |

## Configuration

Configuration is resolved in priority order: **flags > config file > environment variables**.

Copy `workload/example-config.yaml` as a starting point:

```yaml
instance_domain: "https://company.jamfcloud.com"
auth_method: "oauth2"
client_id: ""
client_secret: ""

source_type: "computer_inventory"
strategy: "round-robin"
shard_count: 3

output_format: "json"
```

Environment variables use the prefix `JAMF_`, e.g. `JAMF_CLIENT_SECRET`.

See the full [configuration reference](docs/configuration.md) for every available field.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues and pull requests are welcome.

## License

[MIT](LICENSE)
