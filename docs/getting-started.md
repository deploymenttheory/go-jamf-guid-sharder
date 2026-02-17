# Getting started

## Prerequisites

- A Jamf Pro instance (cloud or on-premise)
- An API credential with at least **read** access to the data you want to shard:
  - For `computer_inventory` / `computer_group_membership`: Computers read
  - For `mobile_device_inventory` / `mobile_device_group_membership`: Mobile Devices read
  - For `user_accounts`: Users read
- One of: OAuth2 API client (recommended), or a Jamf Pro username and password

## Installation

### Download a pre-built binary

Go to the [releases page](https://github.com/deploymenttheory/go-jamf-guid-sharder/releases) and download the archive for your platform.

| Platform | Archive name |
|---|---|
| macOS (Apple Silicon) | `go-jamf-guid-sharder_<version>_darwin_arm64.tar.gz` |
| macOS (Intel) | `go-jamf-guid-sharder_<version>_darwin_amd64.tar.gz` |
| Linux (x86_64) | `go-jamf-guid-sharder_<version>_linux_amd64.tar.gz` |
| Linux (arm64) | `go-jamf-guid-sharder_<version>_linux_arm64.tar.gz` |
| Windows (x86_64) | `go-jamf-guid-sharder_<version>_windows_amd64.zip` |

Each archive contains the binary, `workload/example-config.yaml`, `LICENSE`, and `README.md`.

**macOS / Linux one-liner (arm64 example):**

```bash
curl -L https://github.com/deploymenttheory/go-jamf-guid-sharder/releases/latest/download/go-jamf-guid-sharder_latest_darwin_arm64.tar.gz \
  | tar xz && chmod +x go-jamf-guid-sharder
```

Verify the download against the published checksum file:

```bash
curl -L https://github.com/deploymenttheory/go-jamf-guid-sharder/releases/latest/download/go-jamf-guid-sharder_latest_checksums.txt \
  | sha256sum --check --ignore-missing
```

### Build from source

```bash
git clone https://github.com/deploymenttheory/go-jamf-guid-sharder.git
cd go-jamf-guid-sharder
go build -o go-jamf-guid-sharder ./...
```

## Setting up credentials

### OAuth2 (recommended)

1. In Jamf Pro go to **Settings → API roles and clients**.
2. Create an API role with the read permissions needed for your source type.
3. Create an API client assigned to that role and note the **Client ID** and **Client Secret**.

### Basic auth

Use a Jamf Pro local account username and password. Service accounts with minimal permissions are strongly recommended over admin credentials.

## Your first shard run

The fastest way to get started is with flags:

```bash
go-jamf-guid-sharder shard \
  --instance-domain company.jamfcloud.com \
  --auth-method oauth2 \
  --client-id <client_id> \
  --client-secret <client_secret> \
  --source-type computer_inventory \
  --strategy round-robin \
  --shard-count 3
```

This splits all managed computers into three equal shards and prints the result as JSON to stdout.

## Using a config file

Running with flags every time is noisy. Copy the bundled example config and fill in your values:

```bash
cp workload/example-config.yaml go-jamf-guid-sharder.yaml
$EDITOR go-jamf-guid-sharder.yaml
```

A minimal config:

```yaml
instance_domain: "company.jamfcloud.com"
auth_method: "oauth2"
client_id: "your-client-id"
client_secret: "your-client-secret"

source_type: "computer_inventory"
strategy: "round-robin"
shard_count: 3

output_format: "json"
```

By default the tool looks for `go-jamf-guid-sharder.yaml` in the current directory. Run without any flags:

```bash
go-jamf-guid-sharder shard
```

Pass a different path with `--config`:

```bash
go-jamf-guid-sharder shard --config /etc/sharder/production.yaml
```

## Using environment variables

All config fields can be set via environment variables with the prefix `JAMF_`:

```bash
export JAMF_INSTANCE_DOMAIN="company.jamfcloud.com"
export JAMF_AUTH_METHOD="oauth2"
export JAMF_CLIENT_ID="..."
export JAMF_CLIENT_SECRET="..."

go-jamf-guid-sharder shard --source-type computer_inventory --strategy round-robin --shard-count 3
```

Environment variable names are the `JAMF_` prefix followed by the config key in upper-case with dots replaced by underscores, e.g. `max_retry_attempts` → `JAMF_MAX_RETRY_ATTEMPTS`.

## Piping output

The JSON output is designed to be piped directly into `jq` or other tools:

```bash
# Extract just shard_0
go-jamf-guid-sharder shard --config config.yaml | jq '.shards.shard_0'

# Count devices per shard
go-jamf-guid-sharder shard --config config.yaml | jq '.shards | map_values(length)'

# Write to a file
go-jamf-guid-sharder shard --config config.yaml --output-file shards.json
```

## Next steps

- [Configuration reference](configuration.md) — every field explained
- [Sharding strategies](strategies.md) — choose the right algorithm for your rollout
- [Examples](examples.md) — common real-world patterns
