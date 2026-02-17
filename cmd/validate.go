package cmd

// validate.go mirrors the cross-field validation rules defined in the
// Terraform schema for the jamfpro_guid_list_sharder data source.
//
// Design principles (matching the Terraform approach):
//   - All errors are collected before returning so the user sees everything at once.
//   - Each check is scoped to a single concern and named after its Terraform
//     validator equivalent.
//   - Cross-field conflicts are validated statically (before any API call),
//     not deferred to runtime.

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	// numericIDRe matches IDs that are plain integers — the only format Jamf
	// Pro uses for computer, mobile device, group, and user identifiers.
	// Equivalent to the stringvalidator.RegexMatches(^\d+$) validators applied
	// to group_id, exclude_ids elements, and reserved_ids value elements.
	numericIDRe = regexp.MustCompile(`^\d+$`)

	// shardNameRe matches the shard_N key format expected by reserved_ids.
	// Equivalent to the mapvalidator.KeysAre(RegexMatches(^shard_\d+$)) rule.
	shardNameRe = regexp.MustCompile(`^shard_\d+$`)
)

// validateShardConfig runs all validation rules and returns a combined error
// listing every problem found. Callers receive the full picture in one pass
// rather than having to fix-and-retry one issue at a time.
func validateShardConfig(cfg *shardConfig) error {
	var issues []string

	validateAuth(cfg, &issues)
	validateSource(cfg, &issues)
	validateShardingParameters(cfg, &issues)
	validateIDFormats(cfg, &issues)
	validateIDConflicts(cfg, &issues)
	validateOutput(cfg, &issues)

	if len(issues) == 0 {
		return nil
	}

	return fmt.Errorf(
		"configuration validation failed with %d error(s):\n  • %s",
		len(issues),
		strings.Join(issues, "\n  • "),
	)
}

// ── Auth ──────────────────────────────────────────────────────────────────────

// validateAuth checks that a complete and consistent credential set is present.
func validateAuth(cfg *shardConfig, issues *[]string) {
	if cfg.InstanceDomain == "" {
		*issues = append(*issues, "instance_domain is required")
	}

	switch cfg.AuthMethod {
	case "oauth2":
		if cfg.ClientID == "" {
			*issues = append(*issues, "client_id is required when auth_method is 'oauth2'")
		}
		if cfg.ClientSecret == "" {
			*issues = append(*issues, "client_secret is required when auth_method is 'oauth2'")
		}
		// Warn about ignored basic-auth fields to help catch copy-paste errors.
		if cfg.Username != "" || cfg.Password != "" {
			*issues = append(*issues,
				"basic_auth_username / basic_auth_password are set but auth_method is 'oauth2' — these fields are ignored; remove them or switch auth_method to 'basic'")
		}
	case "basic":
		if cfg.Username == "" {
			*issues = append(*issues, "basic_auth_username is required when auth_method is 'basic'")
		}
		if cfg.Password == "" {
			*issues = append(*issues, "basic_auth_password is required when auth_method is 'basic'")
		}
		// Mirror check for ignored oauth2 fields.
		if cfg.ClientID != "" || cfg.ClientSecret != "" {
			*issues = append(*issues,
				"client_id / client_secret are set but auth_method is 'basic' — these fields are ignored; remove them or switch auth_method to 'oauth2'")
		}
	case "":
		*issues = append(*issues, "auth_method is required: must be 'oauth2' or 'basic'")
	default:
		*issues = append(*issues,
			fmt.Sprintf("auth_method %q is not valid: must be 'oauth2' or 'basic'", cfg.AuthMethod))
	}
}

// ── Source type ───────────────────────────────────────────────────────────────

// validateSource checks source_type membership and group_id requirements.
//
// Terraform equivalents:
//   - stringvalidator.OneOf on source_type
//   - validate.RequiredWhenOneOf("source_type", "computer_group_membership", …) on group_id
//   - stringvalidator.RegexMatches(^\d+$) on group_id
func validateSource(cfg *shardConfig, issues *[]string) {
	validSources := []string{
		"computer_inventory",
		"mobile_device_inventory",
		"computer_group_membership",
		"mobile_device_group_membership",
		"user_accounts",
	}

	sourceValid := false
	for _, s := range validSources {
		if cfg.SourceType == s {
			sourceValid = true
			break
		}
	}
	if !sourceValid {
		if cfg.SourceType == "" {
			*issues = append(*issues,
				fmt.Sprintf("source_type is required: must be one of %s", quotedList(validSources)))
		} else {
			*issues = append(*issues,
				fmt.Sprintf("source_type %q is not valid: must be one of %s", cfg.SourceType, quotedList(validSources)))
		}
	}

	groupRequired := cfg.SourceType == "computer_group_membership" ||
		cfg.SourceType == "mobile_device_group_membership"

	if groupRequired && cfg.GroupID == "" {
		*issues = append(*issues,
			fmt.Sprintf("group_id is required when source_type is %q", cfg.SourceType))
	}

	if cfg.GroupID != "" {
		// Equivalent to stringvalidator.RegexMatches(^\d+$) on group_id.
		if !numericIDRe.MatchString(cfg.GroupID) {
			*issues = append(*issues,
				fmt.Sprintf("group_id %q must be a numeric ID (e.g. \"42\")", cfg.GroupID))
		}
		// Surface a likely mistake: group_id supplied but it will be ignored.
		if !groupRequired && sourceValid {
			*issues = append(*issues,
				fmt.Sprintf("group_id is set (%q) but source_type %q does not use a group — "+
					"set source_type to 'computer_group_membership' or 'mobile_device_group_membership', "+
					"or remove group_id", cfg.GroupID, cfg.SourceType))
		}
	}
}

// ── Sharding parameters ───────────────────────────────────────────────────────

// validateShardingParameters enforces the ExactlyOneOf constraint between
// shard_count, shard_percentages, and shard_sizes, then validates each
// parameter's internal constraints and its relationship to strategy.
func validateShardingParameters(cfg *shardConfig, issues *[]string) {
	hasCount := cfg.ShardCount > 0
	hasPct := len(cfg.ShardPercentages) > 0
	hasSizes := len(cfg.ShardSizes) > 0

	// ── ExactlyOneOf: shard_count / shard_percentages / shard_sizes ───────────
	setCount := 0
	var setNames []string
	if hasCount {
		setCount++
		setNames = append(setNames, fmt.Sprintf("shard_count (%d)", cfg.ShardCount))
	}
	if hasPct {
		setCount++
		setNames = append(setNames, fmt.Sprintf("shard_percentages (%v)", cfg.ShardPercentages))
	}
	if hasSizes {
		setCount++
		setNames = append(setNames, fmt.Sprintf("shard_sizes (%v)", cfg.ShardSizes))
	}

	if setCount == 0 {
		*issues = append(*issues,
			"exactly one of shard_count, shard_percentages, or shard_sizes must be set — none were provided")
	} else if setCount > 1 {
		*issues = append(*issues,
			fmt.Sprintf("exactly one of shard_count, shard_percentages, or shard_sizes must be set — "+
				"multiple were provided: %s", strings.Join(setNames, "; ")))
		// Stop further strategy-specific checks: the param set is ambiguous.
		return
	}

	// ── Strategy validation ───────────────────────────────────────────────────
	validStrategies := []string{"round-robin", "percentage", "size", "rendezvous"}
	strategyValid := false
	for _, s := range validStrategies {
		if cfg.Strategy == s {
			strategyValid = true
			break
		}
	}
	if !strategyValid {
		if cfg.Strategy == "" {
			*issues = append(*issues,
				fmt.Sprintf("strategy is required: must be one of %s", quotedList(validStrategies)))
		} else {
			*issues = append(*issues,
				fmt.Sprintf("strategy %q is not valid: must be one of %s", cfg.Strategy, quotedList(validStrategies)))
		}
		// Cannot check strategy-parameter compatibility without a valid strategy.
		return
	}

	// ── Strategy ↔ parameter compatibility ───────────────────────────────────
	// validate.Int64RequiredWhenOneOf / validate.ListRequiredWhenEquals
	switch cfg.Strategy {
	case "round-robin", "rendezvous":
		if !hasCount {
			*issues = append(*issues,
				fmt.Sprintf("strategy %q requires shard_count — use shard_count, not shard_percentages or shard_sizes",
					cfg.Strategy))
		}
		if hasPct {
			*issues = append(*issues,
				fmt.Sprintf("shard_percentages is set but strategy is %q — shard_percentages is only valid with strategy 'percentage'",
					cfg.Strategy))
		}
		if hasSizes {
			*issues = append(*issues,
				fmt.Sprintf("shard_sizes is set but strategy is %q — shard_sizes is only valid with strategy 'size'",
					cfg.Strategy))
		}

	case "percentage":
		if !hasPct {
			*issues = append(*issues,
				"strategy 'percentage' requires shard_percentages — use shard_percentages, not shard_count or shard_sizes")
		}
		if hasCount {
			*issues = append(*issues,
				"shard_count is set but strategy is 'percentage' — shard_count is only valid with strategies 'round-robin' or 'rendezvous'")
		}
		if hasSizes {
			*issues = append(*issues,
				"shard_sizes is set but strategy is 'percentage' — shard_sizes is only valid with strategy 'size'")
		}

	case "size":
		if !hasSizes {
			*issues = append(*issues,
				"strategy 'size' requires shard_sizes — use shard_sizes, not shard_count or shard_percentages")
		}
		if hasCount {
			*issues = append(*issues,
				"shard_count is set but strategy is 'size' — shard_count is only valid with strategies 'round-robin' or 'rendezvous'")
		}
		if hasPct {
			*issues = append(*issues,
				"shard_percentages is set but strategy is 'size' — shard_percentages is only valid with strategy 'percentage'")
		}
	}

	// ── shard_count internal constraints ─────────────────────────────────────
	// int64validator.AtLeast(1)
	if hasCount && cfg.ShardCount < 1 {
		*issues = append(*issues,
			fmt.Sprintf("shard_count must be at least 1, got %d", cfg.ShardCount))
	}

	// ── shard_percentages internal constraints ────────────────────────────────
	if hasPct {
		// listvalidator.ValueInt64sAre(int64validator.AtLeast(0))
		for i, p := range cfg.ShardPercentages {
			if p < 0 {
				*issues = append(*issues,
					fmt.Sprintf("shard_percentages[%d] is %d — each percentage must be >= 0", i, p))
			}
		}
		// validate.ListInt64SumEquals(100)
		sum := 0
		for _, p := range cfg.ShardPercentages {
			sum += p
		}
		if sum != 100 {
			*issues = append(*issues,
				fmt.Sprintf("shard_percentages must sum to exactly 100, got %d (%v)", sum, cfg.ShardPercentages))
		}
	}

	// ── shard_sizes internal constraints ─────────────────────────────────────
	if hasSizes {
		for i, s := range cfg.ShardSizes {
			// listvalidator.ValueInt64sAre(Any(AtLeast(1), OneOf(-1)))
			if s != -1 && s < 1 {
				*issues = append(*issues,
					fmt.Sprintf("shard_sizes[%d] is %d — each size must be >= 1 or exactly -1 (remainder)", i, s))
			}
			// Only the last element may be -1.
			if s == -1 && i != len(cfg.ShardSizes)-1 {
				*issues = append(*issues,
					fmt.Sprintf("shard_sizes[%d] is -1 (remainder) but is not the last element — "+
						"-1 is only valid in the final position", i))
			}
		}
	}
}

// ── ID format validation ──────────────────────────────────────────────────────

// validateIDFormats checks that every ID-like field contains only numeric
// values, matching the RegexMatches(^\d+$) validators in the Terraform schema.
func validateIDFormats(cfg *shardConfig, issues *[]string) {
	// exclude_ids — each element must be a numeric string.
	for i, id := range cfg.ExcludeIDs {
		if !numericIDRe.MatchString(id) {
			*issues = append(*issues,
				fmt.Sprintf("exclude_ids[%d] %q must be a numeric ID (e.g. \"42\")", i, id))
		}
	}

	// reserved_ids keys — must match shard_N format.
	// reserved_ids values — each ID in each list must be numeric.
	for key, ids := range cfg.ReservedIDs {
		if !shardNameRe.MatchString(key) {
			*issues = append(*issues,
				fmt.Sprintf("reserved_ids key %q is not valid — keys must be in the format 'shard_0', 'shard_1', etc.", key))
		}
		for i, id := range ids {
			if !numericIDRe.MatchString(id) {
				*issues = append(*issues,
					fmt.Sprintf("reserved_ids[%q][%d] %q must be a numeric ID (e.g. \"42\")", key, i, id))
			}
		}
	}
}

// ── Cross-list conflict detection ─────────────────────────────────────────────

// validateIDConflicts detects IDs that appear in both exclude_ids and
// reserved_ids. The Terraform schema documents this as a hard error:
// "exclusion takes precedence — please remove it from reserved_ids."
//
// We also surface duplicate IDs within reserved_ids (across different shards)
// here as a pre-flight check rather than leaving it to applyReservations.
func validateIDConflicts(cfg *shardConfig, issues *[]string) {
	if len(cfg.ExcludeIDs) == 0 && len(cfg.ReservedIDs) == 0 {
		return
	}

	// Build exclude set for O(1) lookup.
	excludeSet := make(map[string]bool, len(cfg.ExcludeIDs))
	for _, id := range cfg.ExcludeIDs {
		excludeSet[id] = true
	}

	// Check each reserved ID against the exclude set and for cross-shard
	// duplicates within reserved_ids.
	seenReserved := make(map[string]string) // id → first shard that claimed it

	for shardName, ids := range cfg.ReservedIDs {
		for _, id := range ids {
			// exclude_ids ∩ reserved_ids conflict.
			if excludeSet[id] {
				*issues = append(*issues,
					fmt.Sprintf("ID %q appears in both exclude_ids and reserved_ids[%q] — "+
						"exclusion takes precedence and the ID will be absent from all shards; "+
						"remove it from reserved_ids or from exclude_ids", id, shardName))
			}
			// Cross-shard duplicate within reserved_ids.
			if prev, seen := seenReserved[id]; seen {
				*issues = append(*issues,
					fmt.Sprintf("ID %q is reserved in multiple shards: %q and %q — "+
						"each ID may only be pinned to one shard", id, prev, shardName))
			} else {
				seenReserved[id] = shardName
			}
		}
	}
}

// ── Output ────────────────────────────────────────────────────────────────────

// validateOutput checks that the output configuration is consistent.
func validateOutput(cfg *shardConfig, issues *[]string) {
	if cfg.OutputFormat != "json" && cfg.OutputFormat != "yaml" {
		if cfg.OutputFormat == "" {
			*issues = append(*issues, "output_format is required: must be 'json' or 'yaml'")
		} else {
			*issues = append(*issues,
				fmt.Sprintf("output_format %q is not valid: must be 'json' or 'yaml'", cfg.OutputFormat))
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// quotedList formats a string slice as a human-readable quoted list,
// e.g. ["round-robin", "percentage", "size", "rendezvous"].
func quotedList(items []string) string {
	quoted := make([]string, len(items))
	for i, s := range items {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
