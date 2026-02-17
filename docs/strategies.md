# Sharding strategies

## Choosing a strategy

| Strategy | Use when |
|---|---|
| `round-robin` | You want equal shard sizes and simplicity is more important than stability |
| `percentage` | Shard sizes should be proportional to the whole fleet (e.g. 5% pilot, 20% early, 75% broad) |
| `size` | Shard sizes are defined by a fixed device count, not a percentage |
| `rendezvous` | Consistency matters — devices should stay in the same shard even as fleet size changes |

---

## round-robin

**Requires:** `shard_count`

Distributes IDs in circular order: ID 0 → shard 0, ID 1 → shard 1, … ID n → shard n%count. Guarantees shard sizes differ by at most 1.

When a `seed` is provided, IDs are sorted numerically then shuffled deterministically before distribution. Without a seed, IDs are distributed in the order returned by the Jamf Pro API.

**Config:**

```yaml
strategy: "round-robin"
shard_count: 3
seed: "os-update-wave-1"   # optional — remove for API order
```

**Output (900 devices, 3 shards):**

```json
{
  "shards": {
    "shard_0": ["1", "4", "7", "10"],
    "shard_1": ["2", "5", "8", "11"],
    "shard_2": ["3", "6", "9", "12"]
  }
}
```

**Stability:** Adding or removing a device changes which shard every device after it lands in. Not suitable for rollouts where you need devices to stay in their assigned shard over time. Use `rendezvous` for that.

---

## percentage

**Requires:** `shard_percentages` (list of integers summing to exactly 100)

Allocates proportional slices of the total fleet. The last shard absorbs any rounding remainder. Reserved IDs are accounted for when calculating target sizes so the final distribution matches the requested percentages as closely as possible.

**Config:**

```yaml
strategy: "percentage"
shard_percentages: [5, 20, 75]
seed: "q2-rollout"
```

**Output (1000 devices, [5, 20, 75]):**

```json
{
  "metadata": {
    "shard_count": 3
  },
  "shards": {
    "shard_0": 50,    // ~50 devices (5%)
    "shard_1": 200,   // ~200 devices (20%)
    "shard_2": 750    // remaining (75%)
  }
}
```

**Stability:** Shard boundaries shift as fleet size changes. Use `rendezvous` if you need stable assignment across fleet changes.

---

## size

**Requires:** `shard_sizes` (list of absolute counts; `-1` in final position means "all remaining")

Assigns exact device counts to each shard. Use `-1` as the last element to capture all remaining devices after the fixed shards are filled.

If the fleet shrinks and there are not enough devices to fill all fixed shards, later shards receive fewer devices than requested (they are capped at what is available) rather than returning an error.

**Config:**

```yaml
strategy: "size"
shard_sizes: [50, 200, -1]   # shard_0: 50, shard_1: 200, shard_2: everything else
```

**Config with all fixed sizes (no remainder):**

```yaml
strategy: "size"
shard_sizes: [100, 100, 100]
```

**Stability:** Same as `round-robin` — shard membership shifts as fleet changes. Use when you need to guarantee a specific maximum device count per wave regardless of fleet size.

---

## rendezvous

**Requires:** `shard_count`

Uses [Highest Random Weight (HRW) hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing). For each device ID, a weight is computed for every shard candidate by hashing `"<id>:shard_<n>:<seed>"` with SHA-256. The device is assigned to the shard with the highest weight.

The seed is always included in the hash input — even when `seed` is an empty string — so the distribution is always deterministic for a given `(id, shard_count, seed)` triple.

**Config:**

```yaml
strategy: "rendezvous"
shard_count: 4
seed: "fleet-segmentation-v2"   # can be any stable string
```

**Why rendezvous?**

When the shard count changes from N to N+1, only ~1/(N+1) of devices change shard. With `round-robin`, nearly all devices would shift. This makes `rendezvous` the right choice for long-running segmentation schemes where devices need to stay in their assigned shard across fleet fluctuations and shard count changes.

**Seed guidance:** Choose a seed that is stable for the lifetime of your segmentation scheme. Changing the seed is equivalent to reshuffling the entire fleet. Use a descriptive, versioned string like `"mdm-rollout-2024-q3"` rather than something that might change.

---

## Exclusions and reservations

These apply to all strategies before distribution begins.

**Exclusions** (`exclude_ids`) — IDs are removed from the pool entirely. They will not appear in any shard.

```yaml
exclude_ids:
  - "1001"   # lab machine
  - "1002"   # kiosk — should never receive updates
```

**Reservations** (`reserved_ids`) — IDs are pinned to a specific shard. They are removed from the general pool first, then appended to their designated shard after the strategy runs. This guarantees a specific device ends up in a specific wave regardless of the algorithm.

```yaml
reserved_ids:
  shard_0:         # VIP devices always in the pilot wave
    - "101"
    - "102"
  shard_2:         # Specific servers pinned to broad deployment
    - "201"
```

Constraints:

- A shard name must be `shard_N` where N is within the shard count range (0 to shard_count-1).
- An ID cannot be reserved in more than one shard.
- An ID cannot appear in both `exclude_ids` and `reserved_ids` — the validator rejects this.

---

## Seeding and reproducibility

When `seed` is set and the strategy is `round-robin`, `percentage`, or `size`, IDs are sorted numerically then shuffled using a deterministic Fisher-Yates shuffle seeded from a SHA-256 hash of the seed string. This means:

- The same seed + same fleet → always the same shard assignment.
- Removing a device from `exclude_ids` or adding a new device will change the shuffle result, but a stable seed makes the change predictable.

For `rendezvous`, the seed is folded directly into the hash weight computation. A stable seed gives stable per-device assignment regardless of fleet changes, making it inherently more reproducible than the other strategies.
