# Examples

## Phased OS update rollout (computers)

Roll out a macOS update in three waves: 5% pilot, 20% early adopters, 75% broad deployment. Use a stable seed so the same computers are always in the same wave.

**Config file (`os-update.yaml`):**

```yaml
instance_domain: "company.jamfcloud.com"
auth_method: "oauth2"
client_id: "your-client-id"
client_secret: "your-client-secret"

source_type: "computer_inventory"
strategy: "percentage"
shard_percentages: [5, 20, 75]
seed: "macos-sequoia-rollout"

# IT test machines — always excluded
exclude_ids:
  - "1001"
  - "1002"

# C-suite laptops — always in the broad wave, never early
reserved_ids:
  shard_2:
    - "501"
    - "502"

output_format: "json"
output_file: "waves.json"
```

```bash
go-jamf-guid-sharder shard --config os-update.yaml
```

The resulting `waves.json` can be consumed by a Jamf Pro policy scope or Terraform resource.

---

## Group-based targeting (mobile devices)

Shard only the members of a specific mobile device group — for example, devices enrolled in a beta programme.

```bash
go-jamf-guid-sharder shard \
  --config base.yaml \
  --source-type mobile_device_group_membership \
  --group-id 42 \
  --strategy round-robin \
  --shard-count 2 \
  --output json | jq '.shards'
```

---

## Deterministic rendezvous for long-running segmentation

When device fleet membership changes regularly, use `rendezvous` to keep assignment stable. Devices only move when the shard count itself changes, and even then only ~1/N are reassigned.

**Config file (`segments.yaml`):**

```yaml
instance_domain: "company.jamfcloud.com"
auth_method: "oauth2"
client_id: "your-client-id"
client_secret: "your-client-secret"

source_type: "computer_inventory"
strategy: "rendezvous"
shard_count: 4
seed: "fleet-segment-2024"   # change only if you intentionally want to reshuffle

output_format: "yaml"
```

```bash
go-jamf-guid-sharder shard --config segments.yaml
```

Run this nightly in a CI pipeline. Newly enrolled devices are automatically assigned to a shard; decommissioned devices simply disappear. All existing devices keep their shard assignment.

---

## Fixed-count waves with a remainder shard

Deploy to exactly 100 pilot devices, then 500 early adopters, then everything else.

```yaml
source_type: "computer_inventory"
strategy: "size"
shard_sizes: [100, 500, -1]
seed: "pilot-wave"
```

The `-1` in the final position captures all remaining computers after the first two shards are filled.

---

## Pipe output to jq

Extract shard_0 IDs as a plain newline-delimited list:

```bash
go-jamf-guid-sharder shard --config config.yaml | jq -r '.shards.shard_0[]'
```

Count devices per shard:

```bash
go-jamf-guid-sharder shard --config config.yaml \
  | jq '.shards | to_entries | map({shard: .key, count: (.value | length)})'
```

Check the metadata summary without the full ID lists:

```bash
go-jamf-guid-sharder shard --config config.yaml | jq '.metadata'
```

---

## CI pipeline integration

Generate shard assignments as part of a CI pipeline and pass them to Terraform or a deployment script.

```yaml
# GitHub Actions example
- name: Generate device shards
  run: |
    go-jamf-guid-sharder shard --config config.yaml --output-file shards.json
  env:
    JAMF_CLIENT_ID: ${{ secrets.JAMF_CLIENT_ID }}
    JAMF_CLIENT_SECRET: ${{ secrets.JAMF_CLIENT_SECRET }}

- name: Use shard_0 in deployment
  run: |
    PILOT_IDS=$(jq -r '.shards.shard_0 | join(",")' shards.json)
    echo "Deploying to pilot devices: $PILOT_IDS"
```

---

## YAML output

Prefer YAML for human review or when feeding into tools that consume YAML natively.

```bash
go-jamf-guid-sharder shard --config config.yaml --output yaml
```

```yaml
metadata:
  generated_at: "2024-11-01T09:15:42Z"
  source_type: computer_inventory
  strategy: round-robin
  seed: ""
  total_ids_fetched: 1200
  excluded_id_count: 2
  reserved_id_count: 3
  unreserved_ids_distributed: 1195
  shard_count: 3
shards:
  shard_0:
    - "1"
    - "4"
    - "7"
  shard_1:
    - "2"
    - "5"
    - "8"
  shard_2:
    - "3"
    - "6"
    - "9"
```

---

## User account sharding

Shard all Jamf Pro user accounts — useful for staged rollouts of Jamf Connect configurations or Self Service customisations.

```yaml
source_type: "user_accounts"
strategy: "round-robin"
shard_count: 2
seed: "connect-rollout"
```

```bash
go-jamf-guid-sharder shard --config user-config.yaml
```
