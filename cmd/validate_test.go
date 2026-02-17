package cmd

// validate_test.go contains unit tests for every validation function in
// validate.go. Tests mirror the structure of the validators themselves:
//
//   TestValidateAuth                — credential completeness and cross-method noise
//   TestValidateSource              — source_type membership, group_id requirements
//   TestValidateShardingParameters  — ExactlyOneOf, strategy ↔ param compatibility,
//                                     per-param internal constraints
//   TestValidateIDFormats           — numeric ID and shard-name regex checks
//   TestValidateIDConflicts         — exclude/reserved overlap, cross-shard duplicates
//   TestValidateOutput              — output_format membership
//   TestValidateShardConfig         — integration: all validators run together,
//                                     all errors collected before returning

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Test helpers ──────────────────────────────────────────────────────────────

// baseOAuth2Config returns a minimal, fully-valid oauth2 shardConfig.
// Individual tests modify only the fields they care about.
func baseOAuth2Config() shardConfig {
	return shardConfig{
		InstanceDomain: "test.jamfcloud.com",
		AuthMethod:     "oauth2",
		ClientID:       "client-id-123",
		ClientSecret:   "client-secret-456",
		SourceType:     "computer_inventory",
		Strategy:       "round-robin",
		ShardCount:     3,
		OutputFormat:   "json",
	}
}

// baseBasicConfig returns a minimal, fully-valid basic-auth shardConfig.
func baseBasicConfig() shardConfig {
	return shardConfig{
		InstanceDomain: "test.jamfcloud.com",
		AuthMethod:     "basic",
		Username:       "admin",
		Password:       "s3cr3t",
		SourceType:     "computer_inventory",
		Strategy:       "round-robin",
		ShardCount:     3,
		OutputFormat:   "json",
	}
}

// hasIssueContaining returns true when at least one string in issues contains substr.
func hasIssueContaining(issues []string, substr string) bool {
	for _, issue := range issues {
		if strings.Contains(issue, substr) {
			return true
		}
	}
	return false
}

// assertIssueContains is a test helper that fails if no issue contains substr.
func assertIssueContains(t *testing.T, issues []string, substr string) {
	t.Helper()
	assert.True(t, hasIssueContaining(issues, substr),
		"expected an issue containing %q\nactual issues: %v", substr, issues)
}

// ── validateAuth ──────────────────────────────────────────────────────────────

func TestValidateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        shardConfig
		wantCount  int
		wantSubstr []string // each must appear in some issue
	}{
		// ── Happy paths ────────────────────────────────────────────────────────
		{
			name:      "oauth2 fully populated",
			cfg:       baseOAuth2Config(),
			wantCount: 0,
		},
		{
			name:      "basic fully populated",
			cfg:       baseBasicConfig(),
			wantCount: 0,
		},

		// ── instance_domain ────────────────────────────────────────────────────
		{
			name: "missing instance_domain",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.InstanceDomain = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"instance_domain is required"},
		},

		// ── auth_method ────────────────────────────────────────────────────────
		{
			name: "empty auth_method",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.AuthMethod = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"auth_method is required"},
		},
		{
			name: "invalid auth_method",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.AuthMethod = "api-key"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"auth_method", "api-key", "not valid"},
		},

		// ── oauth2 credential completeness ─────────────────────────────────────
		{
			name: "oauth2 missing client_id",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ClientID = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"client_id is required"},
		},
		{
			name: "oauth2 missing client_secret",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ClientSecret = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"client_secret is required"},
		},
		{
			name: "oauth2 missing both credentials",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ClientID = ""
				c.ClientSecret = ""
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"client_id is required", "client_secret is required"},
		},

		// ── basic credential completeness ──────────────────────────────────────
		{
			name: "basic missing username",
			cfg: func() shardConfig {
				c := baseBasicConfig()
				c.Username = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"basic_auth_username is required"},
		},
		{
			name: "basic missing password",
			cfg: func() shardConfig {
				c := baseBasicConfig()
				c.Password = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"basic_auth_password is required"},
		},
		{
			name: "basic missing both credentials",
			cfg: func() shardConfig {
				c := baseBasicConfig()
				c.Username = ""
				c.Password = ""
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"basic_auth_username is required", "basic_auth_password is required"},
		},

		// ── Cross-method field noise ───────────────────────────────────────────
		{
			name: "oauth2 with basic fields set",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Username = "admin"
				c.Password = "pass"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"basic_auth_username", "basic_auth_password", "auth_method is 'oauth2'"},
		},
		{
			name: "oauth2 with only username set (not password)",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Username = "admin"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"basic_auth_username"},
		},
		{
			name: "basic with oauth2 fields set",
			cfg: func() shardConfig {
				c := baseBasicConfig()
				c.ClientID = "cid"
				c.ClientSecret = "csec"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"client_id", "client_secret", "auth_method is 'basic'"},
		},
		{
			name: "basic with only client_id set",
			cfg: func() shardConfig {
				c := baseBasicConfig()
				c.ClientID = "cid"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"client_id"},
		},

		// ── Multiple errors accumulate ─────────────────────────────────────────
		{
			name: "missing instance_domain and both oauth2 credentials",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.InstanceDomain = ""
				c.ClientID = ""
				c.ClientSecret = ""
				return c
			}(),
			wantCount:  3,
			wantSubstr: []string{"instance_domain", "client_id", "client_secret"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var issues []string
			validateAuth(&tt.cfg, &issues)

			assert.Len(t, issues, tt.wantCount)
			for _, sub := range tt.wantSubstr {
				assertIssueContains(t, issues, sub)
			}
		})
	}
}

// ── validateSource ────────────────────────────────────────────────────────────

func TestValidateSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        shardConfig
		wantCount  int
		wantSubstr []string
	}{
		// ── Happy paths ────────────────────────────────────────────────────────
		{name: "computer_inventory", cfg: func() shardConfig { c := baseOAuth2Config(); c.SourceType = "computer_inventory"; return c }(), wantCount: 0},
		{name: "mobile_device_inventory", cfg: func() shardConfig { c := baseOAuth2Config(); c.SourceType = "mobile_device_inventory"; return c }(), wantCount: 0},
		{name: "user_accounts", cfg: func() shardConfig { c := baseOAuth2Config(); c.SourceType = "user_accounts"; return c }(), wantCount: 0},
		{
			name: "computer_group_membership with numeric group_id",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_group_membership"
				c.GroupID = "42"
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "mobile_device_group_membership with numeric group_id",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "mobile_device_group_membership"
				c.GroupID = "7"
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "group_id of '0' is numeric",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_group_membership"
				c.GroupID = "0"
				return c
			}(),
			wantCount: 0,
		},

		// ── source_type validation ─────────────────────────────────────────────
		{
			name: "empty source_type",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"source_type is required"},
		},
		{
			name: "invalid source_type",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "tablets"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"source_type", "tablets", "not valid"},
		},

		// ── group_id requirements ──────────────────────────────────────────────
		{
			name: "computer_group_membership without group_id",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_group_membership"
				c.GroupID = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"group_id is required", "computer_group_membership"},
		},
		{
			name: "mobile_device_group_membership without group_id",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "mobile_device_group_membership"
				c.GroupID = ""
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"group_id is required", "mobile_device_group_membership"},
		},

		// ── group_id format ────────────────────────────────────────────────────
		{
			name: "non-numeric group_id with group source type",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_group_membership"
				c.GroupID = "my-group"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"group_id", "my-group", "numeric"},
		},
		{
			name: "group_id with spaces",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_group_membership"
				c.GroupID = "42 "
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"group_id", "numeric"},
		},
		{
			name: "group_id with leading zeros is still numeric",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_group_membership"
				c.GroupID = "007"
				return c
			}(),
			wantCount: 0,
		},

		// ── group_id set for non-group source type ─────────────────────────────
		{
			name: "group_id set but source_type is computer_inventory",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_inventory"
				c.GroupID = "10"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"group_id", "does not use a group"},
		},
		{
			name: "group_id set but source_type is user_accounts",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "user_accounts"
				c.GroupID = "5"
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"group_id", "does not use a group"},
		},
		{
			name: "non-numeric group_id on non-group source type generates only format error",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.SourceType = "computer_inventory"
				c.GroupID = "not-an-id"
				return c
			}(),
			// invalid format + ignored field = 2 errors
			wantCount:  2,
			wantSubstr: []string{"numeric", "does not use a group"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var issues []string
			validateSource(&tt.cfg, &issues)

			assert.Len(t, issues, tt.wantCount)
			for _, sub := range tt.wantSubstr {
				assertIssueContains(t, issues, sub)
			}
		})
	}
}

// ── validateShardingParameters ────────────────────────────────────────────────

func TestValidateShardingParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        shardConfig
		wantCount  int
		wantSubstr []string
	}{
		// ── ExactlyOneOf: happy paths ──────────────────────────────────────────
		{
			name: "round-robin with shard_count",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "round-robin"
				c.ShardCount = 3
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "rendezvous with shard_count",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "rendezvous"
				c.ShardCount = 5
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "percentage with valid percentages summing to 100",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{10, 30, 60}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "percentage with two equal halves",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{50, 50}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "size with explicit sizes",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{50, 100}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "size with remainder in last position",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{50, 200, -1}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "size with single remainder-only shard",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{-1}
				return c
			}(),
			wantCount: 0,
		},

		// ── ExactlyOneOf: none set ─────────────────────────────────────────────
		{
			// When no shard param is provided, the "none provided" error is
			// emitted but the function does NOT return early — so the strategy
			// compatibility check also fires ("round-robin requires shard_count"),
			// giving the user the full picture in one pass.
			name: "none of shard_count/percentages/sizes set, strategy is round-robin",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ShardCount = 0
				c.ShardPercentages = nil
				c.ShardSizes = nil
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"exactly one", "none were provided", "requires shard_count"},
		},

		// ── ExactlyOneOf: multiple set — early return, no strategy checks ──────
		{
			name: "shard_count and shard_percentages both set",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "round-robin"
				c.ShardCount = 3
				c.ShardPercentages = []int{50, 50}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"exactly one", "multiple were provided", "shard_count", "shard_percentages"},
		},
		{
			name: "shard_count and shard_sizes both set",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "round-robin"
				c.ShardCount = 3
				c.ShardSizes = []int{50, 50}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"exactly one", "multiple were provided", "shard_count", "shard_sizes"},
		},
		{
			name: "shard_percentages and shard_sizes both set",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{50, 50}
				c.ShardSizes = []int{50, 50}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"exactly one", "multiple were provided", "shard_percentages", "shard_sizes"},
		},
		{
			name: "all three set",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ShardCount = 3
				c.ShardPercentages = []int{50, 50}
				c.ShardSizes = []int{50, 50}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"exactly one", "multiple were provided"},
		},

		// ── Strategy: missing/invalid ──────────────────────────────────────────
		{
			name: "missing strategy",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = ""
				c.ShardCount = 3
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"strategy is required"},
		},
		{
			name: "invalid strategy name",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "random"
				c.ShardCount = 3
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"strategy", "random", "not valid"},
		},

		// ── Strategy ↔ param: round-robin ──────────────────────────────────────
		{
			name: "round-robin with percentages instead of count",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "round-robin"
				c.ShardCount = 0
				c.ShardPercentages = []int{50, 50}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"round-robin", "requires shard_count", "shard_percentages is set"},
		},
		{
			name: "round-robin with sizes instead of count",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "round-robin"
				c.ShardCount = 0
				c.ShardSizes = []int{50, 50}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"round-robin", "requires shard_count", "shard_sizes is set"},
		},

		// ── Strategy ↔ param: rendezvous ───────────────────────────────────────
		{
			name: "rendezvous with percentages instead of count",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "rendezvous"
				c.ShardCount = 0
				c.ShardPercentages = []int{50, 50}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"rendezvous", "requires shard_count", "shard_percentages is set"},
		},

		// ── Strategy ↔ param: percentage ──────────────────────────────────────
		{
			name: "percentage with count instead of percentages",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 3
				c.ShardPercentages = nil
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"percentage", "requires shard_percentages", "shard_count is set"},
		},
		{
			name: "percentage with sizes instead of percentages",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardSizes = []int{50, 50}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"percentage", "requires shard_percentages", "shard_sizes is set"},
		},

		// ── Strategy ↔ param: size ─────────────────────────────────────────────
		{
			name: "size with count instead of sizes",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 3
				c.ShardSizes = nil
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"size", "requires shard_sizes", "shard_count is set"},
		},
		{
			name: "size with percentages instead of sizes",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardPercentages = []int{50, 50}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"size", "requires shard_sizes", "shard_percentages is set"},
		},

		// ── shard_percentages internal ─────────────────────────────────────────
		{
			// -10 + 60 = 50 ≠ 100, so both the negative-value and wrong-sum checks fire.
			name: "percentages with one negative value and wrong sum",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{-10, 60}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"shard_percentages[0]", "-10", ">= 0", "must sum to exactly 100"},
		},
		{
			name: "percentages summing to less than 100",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{10, 20}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"must sum to exactly 100", "got 30"},
		},
		{
			name: "percentages summing to more than 100",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{60, 60}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"must sum to exactly 100", "got 120"},
		},
		{
			// -5 + -5 + 90 = 80 ≠ 100, so all three checks fire: index 0 negative,
			// index 1 negative, sum wrong.
			name: "multiple negative percentages with wrong sum",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{-5, -5, 90}
				return c
			}(),
			wantCount:  3,
			wantSubstr: []string{"shard_percentages[0]", "shard_percentages[1]", "must sum to exactly 100"},
		},
		{
			name: "zero percentage is allowed (>= 0)",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "percentage"
				c.ShardCount = 0
				c.ShardPercentages = []int{0, 100}
				return c
			}(),
			wantCount: 0,
		},

		// ── shard_sizes internal ───────────────────────────────────────────────
		{
			name: "sizes with zero value",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{0, 100}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"shard_sizes[0]", "0", ">= 1"},
		},
		{
			name: "sizes with negative non-minus-one value",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{-2, 100}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"shard_sizes[0]", "-2", ">= 1"},
		},
		{
			name: "minus-one in non-last position",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{-1, 50, 100}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"shard_sizes[0]", "-1", "not the last element"},
		},
		{
			name: "minus-one in middle of three elements",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{50, -1, 100}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"shard_sizes[1]", "-1", "not the last element"},
		},
		{
			name: "multiple invalid sizes accumulate",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.Strategy = "size"
				c.ShardCount = 0
				c.ShardSizes = []int{0, -2, 50}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"shard_sizes[0]", "shard_sizes[1]"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var issues []string
			validateShardingParameters(&tt.cfg, &issues)

			assert.Len(t, issues, tt.wantCount)
			for _, sub := range tt.wantSubstr {
				assertIssueContains(t, issues, sub)
			}
		})
	}
}

// ── validateIDFormats ─────────────────────────────────────────────────────────

func TestValidateIDFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        shardConfig
		wantCount  int
		wantSubstr []string
	}{
		// ── Happy paths ────────────────────────────────────────────────────────
		{
			name:      "empty exclude_ids and reserved_ids",
			cfg:       baseOAuth2Config(),
			wantCount: 0,
		},
		{
			name: "all numeric exclude_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"1", "42", "100", "9999"}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "valid reserved_ids: shard_N keys with numeric values",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{
					"shard_0": {"101", "102"},
					"shard_2": {"201"},
				}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "shard_10 is a valid key",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"shard_10": {"5"}}
				return c
			}(),
			wantCount: 0,
		},

		// ── exclude_ids format ─────────────────────────────────────────────────
		{
			name: "non-numeric in exclude_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"abc"}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"exclude_ids[0]", "abc", "numeric"},
		},
		{
			name: "exclude_ids with UUID-style value",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"550e8400-e29b-41d4-a716-446655440000"}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"exclude_ids[0]", "numeric"},
		},
		{
			name: "multiple non-numeric exclude_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"abc", "123", "def"}
				return c
			}(),
			wantCount:  2, // index 0 and 2 are bad; index 1 is fine
			wantSubstr: []string{"exclude_ids[0]", "exclude_ids[2]"},
		},

		// ── reserved_ids key format ────────────────────────────────────────────
		{
			name: "reserved_ids with bare key (no shard_ prefix)",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"0": {"1"}}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"reserved_ids key", "0", "shard_0"},
		},
		{
			name: "reserved_ids with 'group_0' key (wrong prefix)",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"group_0": {"1"}}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"reserved_ids key", "group_0"},
		},
		{
			name: "reserved_ids with shard_abc key",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"shard_abc": {"1"}}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"reserved_ids key", "shard_abc"},
		},

		// ── reserved_ids value format ──────────────────────────────────────────
		{
			name: "non-numeric ID value in reserved_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"shard_0": {"notanumber"}}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{`reserved_ids["shard_0"][0]`, "notanumber", "numeric"},
		},
		{
			name: "mixed valid and invalid values in reserved_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"shard_0": {"101", "bad", "103"}}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{`reserved_ids["shard_0"][1]`, "bad"},
		},

		// ── Combined key and value errors ──────────────────────────────────────
		{
			name: "invalid key AND invalid value",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"badkey": {"notanumber"}}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"reserved_ids key", "badkey", "numeric"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var issues []string
			validateIDFormats(&tt.cfg, &issues)

			assert.Len(t, issues, tt.wantCount)
			for _, sub := range tt.wantSubstr {
				assertIssueContains(t, issues, sub)
			}
		})
	}
}

// ── validateIDConflicts ───────────────────────────────────────────────────────

func TestValidateIDConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       shardConfig
		wantCount int
		// wantSubstr uses substring matching to avoid coupling to map iteration
		// order (the exact shard names in "X and Y" can vary).
		wantSubstr []string
	}{
		// ── Happy paths ────────────────────────────────────────────────────────
		{
			name:      "both empty — early return",
			cfg:       baseOAuth2Config(),
			wantCount: 0,
		},
		{
			name: "only exclude_ids, no reserved_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"1", "2", "3"}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "only reserved_ids, no exclude_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{"shard_0": {"101"}, "shard_1": {"202"}}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "no overlap between exclude_ids and reserved_ids",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"1", "2"}
				c.ReservedIDs = map[string][]string{"shard_0": {"101"}, "shard_1": {"202"}}
				return c
			}(),
			wantCount: 0,
		},
		{
			name: "same ID in two different shards of reserved_ids — duplicate across shards",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{
					"shard_0": {"101", "202"},
					"shard_1": {"303"},
				}
				return c
			}(),
			wantCount: 0, // 101 and 202 are in shard_0, 303 in shard_1 — no overlap
		},

		// ── Exclude ∩ Reserved conflicts ───────────────────────────────────────
		{
			name: "single ID in both exclude and reserved",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"101"}
				c.ReservedIDs = map[string][]string{"shard_0": {"101"}}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"101", "exclude_ids", "reserved_ids"},
		},
		{
			name: "two IDs each in both lists",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"101", "202"}
				c.ReservedIDs = map[string][]string{"shard_0": {"101", "202"}}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"101", "202"},
		},
		{
			name: "conflicting ID spread across reserved shards",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ExcludeIDs = []string{"101"}
				c.ReservedIDs = map[string][]string{
					"shard_0": {"101"},
					"shard_1": {"101"}, // also conflicts with exclude AND is a cross-shard dup
				}
				return c
			}(),
			// 1 exclude∩shard_0 + 1 exclude∩shard_1 + 1 cross-shard dup = 3
			wantCount:  3,
			wantSubstr: []string{"101", "exclude_ids", "multiple shards"},
		},

		// ── Cross-shard duplicates within reserved_ids ─────────────────────────
		{
			name: "same ID reserved in two shards",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{
					"shard_0": {"101"},
					"shard_1": {"101"},
				}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"101", "multiple shards"},
		},
		{
			name: "two different IDs each duplicated across shards",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{
					"shard_0": {"101", "202"},
					"shard_1": {"101", "202"},
				}
				return c
			}(),
			wantCount:  2,
			wantSubstr: []string{"101", "202"},
		},
		{
			name: "multiple IDs — some duplicated, some not",
			cfg: func() shardConfig {
				c := baseOAuth2Config()
				c.ReservedIDs = map[string][]string{
					"shard_0": {"101", "999"},
					"shard_1": {"101", "888"}, // 101 dup, 999/888 unique
				}
				return c
			}(),
			wantCount:  1,
			wantSubstr: []string{"101", "multiple shards"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var issues []string
			validateIDConflicts(&tt.cfg, &issues)

			assert.Len(t, issues, tt.wantCount)
			for _, sub := range tt.wantSubstr {
				assertIssueContains(t, issues, sub)
			}
		})
	}
}

// ── validateOutput ────────────────────────────────────────────────────────────

func TestValidateOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     string
		wantCount  int
		wantSubstr []string
	}{
		{name: "json", format: "json", wantCount: 0},
		{name: "yaml", format: "yaml", wantCount: 0},
		{
			name: "empty format",
			format: "",
			wantCount:  1,
			wantSubstr: []string{"output_format is required"},
		},
		{
			name: "toml is not valid",
			format: "toml",
			wantCount:  1,
			wantSubstr: []string{"output_format", "toml", "not valid"},
		},
		{
			name: "JSON uppercase is not valid",
			format: "JSON",
			wantCount:  1,
			wantSubstr: []string{"output_format", "JSON"},
		},
		{
			name: "xml is not valid",
			format: "xml",
			wantCount:  1,
			wantSubstr: []string{"output_format", "xml"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := baseOAuth2Config()
			cfg.OutputFormat = tt.format

			var issues []string
			validateOutput(&cfg, &issues)

			assert.Len(t, issues, tt.wantCount)
			for _, sub := range tt.wantSubstr {
				assertIssueContains(t, issues, sub)
			}
		})
	}
}

// ── validateShardConfig (integration) ────────────────────────────────────────

func TestValidateShardConfig(t *testing.T) {
	t.Parallel()

	t.Run("fully valid oauth2 round-robin config returns no error", func(t *testing.T) {
		t.Parallel()
		cfg := baseOAuth2Config()
		require.NoError(t, validateShardConfig(&cfg))
	})

	t.Run("fully valid basic percentage config returns no error", func(t *testing.T) {
		t.Parallel()
		cfg := baseBasicConfig()
		cfg.Strategy = "percentage"
		cfg.ShardCount = 0
		cfg.ShardPercentages = []int{10, 30, 60}
		require.NoError(t, validateShardConfig(&cfg))
	})

	t.Run("fully valid rendezvous config with seed returns no error", func(t *testing.T) {
		t.Parallel()
		cfg := baseOAuth2Config()
		cfg.Strategy = "rendezvous"
		cfg.ShardCount = 4
		cfg.Seed = "os-update-wave"
		require.NoError(t, validateShardConfig(&cfg))
	})

	t.Run("fully valid size config with remainder returns no error", func(t *testing.T) {
		t.Parallel()
		cfg := baseOAuth2Config()
		cfg.Strategy = "size"
		cfg.ShardCount = 0
		cfg.ShardSizes = []int{50, 200, -1}
		require.NoError(t, validateShardConfig(&cfg))
	})

	t.Run("all errors from different validators are collected together", func(t *testing.T) {
		t.Parallel()
		// Deliberately break auth, source, sharding, ID format, and output.
		cfg := shardConfig{
			AuthMethod:     "token",       // invalid auth method
			SourceType:     "printers",    // invalid source type
			Strategy:       "round-robin",
			ShardCount:     3,
			ExcludeIDs:     []string{"not-an-id"},  // non-numeric
			OutputFormat:   "csv",                  // invalid output
		}

		err := validateShardConfig(&cfg)
		require.Error(t, err)

		msg := err.Error()
		assert.Contains(t, msg, "instance_domain is required")
		assert.Contains(t, msg, "auth_method")
		assert.Contains(t, msg, "source_type")
		assert.Contains(t, msg, "exclude_ids")
		assert.Contains(t, msg, "output_format")

		// Error header should state count (at least 5 issues above).
		assert.Contains(t, msg, "error(s)")
	})

	t.Run("empty config produces errors for every required field", func(t *testing.T) {
		t.Parallel()
		cfg := shardConfig{}
		err := validateShardConfig(&cfg)
		require.Error(t, err)

		msg := err.Error()
		assert.Contains(t, msg, "instance_domain")
		assert.Contains(t, msg, "auth_method")
		assert.Contains(t, msg, "source_type")
		assert.Contains(t, msg, "exactly one")
		assert.Contains(t, msg, "strategy")
		assert.Contains(t, msg, "output_format")
	})

	t.Run("exclude and reserved conflict is caught before API call", func(t *testing.T) {
		t.Parallel()
		cfg := baseOAuth2Config()
		cfg.ExcludeIDs = []string{"101"}
		cfg.ReservedIDs = map[string][]string{"shard_0": {"101"}}

		err := validateShardConfig(&cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "101")
		assert.Contains(t, err.Error(), "exclude_ids")
	})

	t.Run("shard_count and shard_percentages mutual exclusivity", func(t *testing.T) {
		t.Parallel()
		cfg := baseOAuth2Config()
		cfg.Strategy = "round-robin"
		cfg.ShardCount = 3
		cfg.ShardPercentages = []int{50, 50}

		err := validateShardConfig(&cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one")
		assert.Contains(t, err.Error(), "shard_count")
		assert.Contains(t, err.Error(), "shard_percentages")
	})

	t.Run("error message includes bullet-point count header", func(t *testing.T) {
		t.Parallel()
		cfg := baseOAuth2Config()
		cfg.ClientID = ""
		cfg.ClientSecret = ""

		err := validateShardConfig(&cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "2 error(s)")
		assert.Contains(t, err.Error(), "•")
	})
}
