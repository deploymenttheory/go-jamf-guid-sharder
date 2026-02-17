package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/deploymenttheory/go-api-sdk-jamfpro/sdk/jamfpro"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var shardCmd = &cobra.Command{
	Use:   "shard",
	Short: "Retrieve Jamf Pro IDs and distribute them into shards",
	Long: `Connects to Jamf Pro, fetches device or user IDs from the specified source,
applies any exclusions and reservations, then distributes the IDs using the
chosen sharding strategy. Output is written as JSON or YAML to stdout or a file.

Examples:
  # Round-robin with 3 shards, deterministic via seed
  go-jamf-guid-sharder shard \
    --instance-domain company.jamfcloud.com \
    --auth-method oauth2 --client-id abc --client-secret xyz \
    --source-type computer_inventory \
    --strategy round-robin --shard-count 3 --seed os-updates

  # Percentage split piped to jq
  go-jamf-guid-sharder shard --config ./config.yaml \
    --strategy percentage --shard-percentages 10,30,60 \
    --output json | jq '.shards.shard_0'

  # Size-based with remainder shard, YAML output to file
  go-jamf-guid-sharder shard --config ./config.yaml \
    --strategy size --shard-sizes 50,200,-1 \
    --output yaml --output-file shards.yaml`,
	RunE: runShard,
}

func init() {
	rootCmd.AddCommand(shardCmd)

	// ── Authentication ────────────────────────────────────────────────────────
	shardCmd.Flags().String("instance-domain", "", "Jamf Pro instance domain (e.g. company.jamfcloud.com)")
	shardCmd.Flags().String("auth-method", "oauth2", "Authentication method: oauth2 or basic")
	shardCmd.Flags().String("client-id", "", "OAuth2 client ID")
	shardCmd.Flags().String("client-secret", "", "OAuth2 client secret")
	shardCmd.Flags().String("username", "", "Basic auth username")
	shardCmd.Flags().String("password", "", "Basic auth password")

	// ── HTTP client tuning ────────────────────────────────────────────────────
	shardCmd.Flags().String("log-level", "warn", "Log level: debug, info, warn, error, fatal")
	shardCmd.Flags().String("log-export-path", "", "Additional log file path (appended to stderr output)")
	shardCmd.Flags().Bool("hide-sensitive-data", true, "Mask sensitive data in logs")
	shardCmd.Flags().Bool("jamf-load-balancer-lock", false, "Lock all requests to one Jamf Pro load balancer node")
	shardCmd.Flags().Int("max-retry-attempts", 3, "Maximum number of retry attempts per request")
	shardCmd.Flags().Int("max-concurrent-requests", 1, "Maximum number of concurrent API requests")
	shardCmd.Flags().Bool("enable-dynamic-rate-limiting", false, "Enable dynamic rate limiting")
	shardCmd.Flags().Int("custom-timeout", 60, "Per-request timeout in seconds")
	shardCmd.Flags().Int("token-refresh-buffer", 300, "Token refresh buffer period in seconds")
	shardCmd.Flags().Int("total-retry-duration", 60, "Total retry window duration in seconds")
	shardCmd.Flags().Bool("follow-redirects", true, "Follow HTTP redirects")
	shardCmd.Flags().Int("max-redirects", 5, "Maximum number of redirects to follow")
	shardCmd.Flags().Bool("enable-concurrency-management", true, "Enable concurrency management")
	shardCmd.Flags().Int("mandatory-request-delay", 0, "Mandatory delay between requests in milliseconds")
	shardCmd.Flags().Bool("retry-eligible-requests", true, "Retry eligible failed requests")

	// ── Sharding ──────────────────────────────────────────────────────────────
	shardCmd.Flags().String("source-type", "", "Source to query IDs from:\n"+
		"  computer_inventory              — all managed computers\n"+
		"  mobile_device_inventory         — all managed mobile devices\n"+
		"  computer_group_membership       — members of a computer group (requires --group-id)\n"+
		"  mobile_device_group_membership  — members of a mobile device group (requires --group-id)\n"+
		"  user_accounts                   — all Jamf Pro user accounts")
	shardCmd.Flags().String("group-id", "", "Jamf Pro group ID (required for *_group_membership source types)")
	shardCmd.Flags().String("strategy", "", "Sharding strategy: round-robin | percentage | size | rendezvous")
	shardCmd.Flags().Int("shard-count", 0, "Number of shards (required for round-robin and rendezvous)")
	shardCmd.Flags().IntSlice("shard-percentages", []int{}, "Percentages summing to 100, e.g. 10,30,60 (percentage strategy)")
	shardCmd.Flags().IntSlice("shard-sizes", []int{}, "Absolute shard sizes; use -1 as last element for remainder, e.g. 50,200,-1 (size strategy)")
	shardCmd.Flags().String("seed", "", "Seed for deterministic distribution (supported by all strategies)")
	shardCmd.Flags().StringSlice("exclude-ids", []string{}, "IDs to completely exclude from all shards (comma-separated)")
	shardCmd.Flags().String("reserved-ids", "",
		`JSON map of shard names to ID lists to pin to specific shards,
e.g. '{"shard_0":["101","102"],"shard_2":["201"]}'`)

	// ── Output ────────────────────────────────────────────────────────────────
	shardCmd.Flags().StringP("output", "o", "json", "Output format: json or yaml")
	shardCmd.Flags().String("output-file", "", "Write output to this file path instead of stdout")

	bindShardFlags(shardCmd)
}

// bindShardFlags wires cobra flags to viper keys so that flags, env vars,
// and config file values are all resolved through a single viper lookup.
func bindShardFlags(cmd *cobra.Command) {
	pairs := map[string]string{
		"instance-domain":              "instance_domain",
		"auth-method":                  "auth_method",
		"client-id":                    "client_id",
		"client-secret":                "client_secret",
		"username":                     "basic_auth_username",
		"password":                     "basic_auth_password",
		"log-level":                    "log_level",
		"log-export-path":              "log_export_path",
		"hide-sensitive-data":          "hide_sensitive_data",
		"jamf-load-balancer-lock":      "jamf_load_balancer_lock",
		"max-retry-attempts":           "max_retry_attempts",
		"max-concurrent-requests":      "max_concurrent_requests",
		"enable-dynamic-rate-limiting": "enable_dynamic_rate_limiting",
		"custom-timeout":               "custom_timeout_seconds",
		"token-refresh-buffer":         "token_refresh_buffer_period_seconds",
		"total-retry-duration":         "total_retry_duration_seconds",
		"follow-redirects":             "follow_redirects",
		"max-redirects":                "max_redirects",
		"enable-concurrency-management": "enable_concurrency_management",
		"mandatory-request-delay":      "mandatory_request_delay_milliseconds",
		"retry-eligible-requests":      "retry_eligiable_requests",
		"source-type":                  "source_type",
		"group-id":                     "group_id",
		"strategy":                     "strategy",
		"shard-count":                  "shard_count",
		"shard-percentages":            "shard_percentages",
		"shard-sizes":                  "shard_sizes",
		"seed":                         "seed",
		"exclude-ids":                  "exclude_ids",
		"output":                       "output_format",
		"output-file":                  "output_file",
	}
	for flag, key := range pairs {
		if f := cmd.Flags().Lookup(flag); f != nil {
			viper.BindPFlag(key, f) //nolint:errcheck
		}
	}
}

func runShard(cmd *cobra.Command, _ []string) error {
	var cfg shardConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	// viper.Unmarshal can struggle with IntSlice flags bound from cobra; use
	// GetIntSlice / GetStringSlice as a reliable fallback.
	if len(cfg.ShardPercentages) == 0 {
		cfg.ShardPercentages = viper.GetIntSlice("shard_percentages")
	}
	if len(cfg.ShardSizes) == 0 {
		cfg.ShardSizes = viper.GetIntSlice("shard_sizes")
	}
	if len(cfg.ExcludeIDs) == 0 {
		cfg.ExcludeIDs = viper.GetStringSlice("exclude_ids")
	}

	// reserved-ids flag accepts a JSON string on the command line; a config file
	// may supply it as a native YAML/JSON map which viper.Unmarshal handles.
	if rawFlag, _ := cmd.Flags().GetString("reserved-ids"); rawFlag != "" {
		parsed := make(map[string][]string)
		if err := json.Unmarshal([]byte(rawFlag), &parsed); err != nil {
			return fmt.Errorf("invalid --reserved-ids JSON: %w", err)
		}
		cfg.ReservedIDs = parsed
	}
	// If the flag was not set but the config file has reserved_ids, use viper.
	if cfg.ReservedIDs == nil && viper.IsSet("reserved_ids") {
		cfg.ReservedIDs = viper.GetStringMapStringSlice("reserved_ids")
	}

	if err := validateShardConfig(&cfg); err != nil {
		return err
	}

	client, err := buildJamfClient(&cfg)
	if err != nil {
		return fmt.Errorf("failed to build Jamf Pro client: %w", err)
	}

	sourceIDs, err := fetchSourceIDs(client, &cfg)
	if err != nil {
		return err
	}
	totalFetched := len(sourceIDs)

	filteredIDs := applyExclusions(sourceIDs, cfg.ExcludeIDs)
	excludedCount := totalFetched - len(filteredIDs)

	shardCount := resolveShardCount(&cfg)
	reservations, err := applyReservations(filteredIDs, cfg.ReservedIDs, shardCount)
	if err != nil {
		return err
	}
	reservedCount := len(filteredIDs) - len(reservations.UnreservedIDs)

	shards, err := applyStrategy(&cfg, filteredIDs, reservations)
	if err != nil {
		return err
	}

	result := ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:              time.Now().UTC(),
			SourceType:               cfg.SourceType,
			GroupID:                  cfg.GroupID,
			Strategy:                 cfg.Strategy,
			Seed:                     cfg.Seed,
			TotalIDsFetched:          totalFetched,
			ExcludedIDCount:          excludedCount,
			ReservedIDCount:          reservedCount,
			UnreservedIDsDistributed: len(reservations.UnreservedIDs),
			ShardCount:               len(shards),
		},
		Shards: make(map[string][]string, len(shards)),
	}
	for i, shard := range shards {
		result.Shards[fmt.Sprintf("shard_%d", i)] = shard
	}

	return writeOutput(&cfg, &result)
}

// ── Client construction ───────────────────────────────────────────────────────

// buildJamfClient constructs a jamfpro.Client from the resolved shardConfig,
// mapping directly onto jamfpro.ConfigContainer fields.
func buildJamfClient(cfg *shardConfig) (*jamfpro.Client, error) {
	container := &jamfpro.ConfigContainer{
		InstanceDomain:              cfg.InstanceDomain,
		AuthMethod:                  cfg.AuthMethod,
		ClientID:                    cfg.ClientID,
		ClientSecret:                cfg.ClientSecret,
		Username:                    cfg.Username,
		Password:                    cfg.Password,
		LogLevel:                    cfg.LogLevel,
		LogExportPath:               cfg.LogExportPath,
		HideSensitiveData:           cfg.HideSensitiveData,
		JamfLoadBalancerLock:        cfg.JamfLoadBalancerLock,
		MaxRetryAttempts:            cfg.MaxRetryAttempts,
		MaxConcurrentRequests:       cfg.MaxConcurrentRequests,
		EnableDynamicRateLimiting:   cfg.EnableDynamicRateLimiting,
		CustomTimeout:               cfg.CustomTimeout,
		TokenRefreshBufferPeriod:    cfg.TokenRefreshBufferPeriod,
		TotalRetryDuration:          cfg.TotalRetryDuration,
		FollowRedirects:             cfg.FollowRedirects,
		MaxRedirects:                cfg.MaxRedirects,
		EnableConcurrencyManagement: cfg.EnableConcurrencyManagement,
		MandatoryRequestDelay:       cfg.MandatoryRequestDelay,
		RetryEligiableRequests:      cfg.RetryEligiableRequests,
	}
	return jamfpro.BuildClient(container)
}

// ── ID fetching ───────────────────────────────────────────────────────────────

// fetchSourceIDs dispatches to the appropriate Jamf Pro endpoint based on
// the configured source_type.
func fetchSourceIDs(client *jamfpro.Client, cfg *shardConfig) ([]string, error) {
	switch cfg.SourceType {
	case "computer_inventory":
		return fetchComputerInventory(client)
	case "mobile_device_inventory":
		return fetchMobileDeviceInventory(client)
	case "computer_group_membership":
		return fetchComputerGroupMembers(client, cfg.GroupID)
	case "mobile_device_group_membership":
		return fetchMobileDeviceGroupMembers(client, cfg.GroupID)
	case "user_accounts":
		return fetchUsers(client)
	default:
		return nil, fmt.Errorf("unknown source_type: %s", cfg.SourceType)
	}
}

// fetchComputerInventory returns IDs for all managed computers.
// Unmanaged computers are excluded because they cannot be members of a
// Jamf Pro static group.
func fetchComputerInventory(client *jamfpro.Client) ([]string, error) {
	params := url.Values{}
	params.Set("section", "GENERAL")

	computers, err := client.GetComputersInventory(params)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve computer inventory: %w", err)
	}

	var ids []string
	for _, c := range computers.Results {
		if c.General.RemoteManagement.Managed {
			ids = append(ids, c.ID)
		}
	}
	return ids, nil
}

// fetchMobileDeviceInventory returns IDs for all managed mobile devices.
// Unmanaged devices are excluded for the same reason as unmanaged computers.
func fetchMobileDeviceInventory(client *jamfpro.Client) ([]string, error) {
	devices, err := client.GetMobileDevices()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve mobile devices: %w", err)
	}

	var ids []string
	for _, d := range devices.MobileDevices {
		if d.Managed {
			ids = append(ids, strconv.Itoa(d.ID))
		}
	}
	return ids, nil
}

// fetchComputerGroupMembers returns the IDs of all computers in the given group.
func fetchComputerGroupMembers(client *jamfpro.Client, groupID string) ([]string, error) {
	group, err := client.GetComputerGroupByID(groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve computer group %s: %w", groupID, err)
	}

	var ids []string
	if group.Computers != nil {
		for _, c := range *group.Computers {
			ids = append(ids, strconv.Itoa(c.ID))
		}
	}
	return ids, nil
}

// fetchMobileDeviceGroupMembers returns the IDs of all mobile devices in the given group.
func fetchMobileDeviceGroupMembers(client *jamfpro.Client, groupID string) ([]string, error) {
	group, err := client.GetMobileDeviceGroupByID(groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve mobile device group %s: %w", groupID, err)
	}

	var ids []string
	if group.MobileDevices != nil {
		for _, d := range *group.MobileDevices {
			ids = append(ids, strconv.Itoa(d.ID))
		}
	}
	return ids, nil
}

// fetchUsers returns the IDs of all Jamf Pro user accounts.
func fetchUsers(client *jamfpro.Client) ([]string, error) {
	users, err := client.GetUsers()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve users: %w", err)
	}

	var ids []string
	for _, u := range users.Users {
		ids = append(ids, strconv.Itoa(u.ID))
	}
	return ids, nil
}

// ── Exclusions & reservations ─────────────────────────────────────────────────

// applyExclusions removes any ID present in excludeIDs from the pool.
func applyExclusions(ids []string, excludeIDs []string) []string {
	if len(excludeIDs) == 0 {
		return ids
	}
	excludeSet := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		excludeSet[id] = true
	}
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if !excludeSet[id] {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// applyReservations partitions the ID pool into reserved (pinned to a specific
// shard) and unreserved (available for the sharding algorithm). Validates that
// shard names are in range and that no ID appears in more than one shard.
func applyReservations(ids []string, reservedMap map[string][]string, shardCount int) (*shardReservations, error) {
	info := &shardReservations{
		IDsByShard:    make(map[string][]string),
		CountsByShard: make(map[int]int),
		UnreservedIDs: ids,
	}
	if len(reservedMap) == 0 {
		return info, nil
	}

	seenIDs := make(map[string]string)

	for shardName, idList := range reservedMap {
		var shardIndex int
		if _, err := fmt.Sscanf(shardName, "shard_%d", &shardIndex); err != nil {
			return nil, fmt.Errorf("invalid shard name %q in reserved_ids: must be 'shard_0', 'shard_1', etc.", shardName)
		}
		if shardIndex < 0 || shardIndex >= shardCount {
			return nil, fmt.Errorf(
				"shard name %q in reserved_ids is out of range: with shard_count=%d, valid names are shard_0 to shard_%d",
				shardName, shardCount, shardCount-1,
			)
		}
		for _, id := range idList {
			if prev, exists := seenIDs[id]; exists {
				return nil, fmt.Errorf(
					"ID %q appears in multiple reserved_ids shards: %q and %q — each ID may only be reserved for one shard",
					id, prev, shardName,
				)
			}
			seenIDs[id] = shardName
		}
		info.IDsByShard[shardName] = idList
		info.CountsByShard[shardIndex] = len(idList)
	}

	if len(seenIDs) > 0 {
		reservedSet := make(map[string]bool, len(seenIDs))
		for id := range seenIDs {
			reservedSet[id] = true
		}
		filtered := make([]string, 0, len(ids))
		for _, id := range ids {
			if !reservedSet[id] {
				filtered = append(filtered, id)
			}
		}
		info.UnreservedIDs = filtered
	}

	return info, nil
}

// ── Strategy dispatch ─────────────────────────────────────────────────────────

// resolveShardCount infers the shard count from whichever configuration
// parameter is active for the chosen strategy.
func resolveShardCount(cfg *shardConfig) int {
	if len(cfg.ShardPercentages) > 0 {
		return len(cfg.ShardPercentages)
	}
	if len(cfg.ShardSizes) > 0 {
		return len(cfg.ShardSizes)
	}
	return cfg.ShardCount
}

// applyStrategy routes to the appropriate sharding algorithm and returns the
// resulting per-shard ID slices.
func applyStrategy(cfg *shardConfig, ids []string, reservations *shardReservations) ([][]string, error) {
	switch cfg.Strategy {
	case "round-robin":
		return shardByRoundRobin(ids, cfg.ShardCount, cfg.Seed, reservations), nil
	case "rendezvous":
		return shardByRendezvous(ids, cfg.ShardCount, cfg.Seed, reservations), nil
	case "percentage":
		return shardByPercentage(ids, cfg.ShardPercentages, cfg.Seed, reservations), nil
	case "size":
		return shardBySize(ids, cfg.ShardSizes, cfg.Seed, reservations), nil
	default:
		return nil, fmt.Errorf("unknown strategy: %q", cfg.Strategy)
	}
}

// ── Output ────────────────────────────────────────────────────────────────────

// writeOutput serialises the ShardResult to the configured format and writes
// it to stdout or the specified output file.
func writeOutput(cfg *shardConfig, result *ShardResult) error {
	var (
		data []byte
		err  error
	)

	switch cfg.OutputFormat {
	case "yaml":
		data, err = yaml.Marshal(result)
	default: // json
		data, err = json.MarshalIndent(result, "", "  ")
		if err == nil {
			data = append(data, '\n')
		}
	}
	if err != nil {
		return fmt.Errorf("failed to marshal output as %s: %w", cfg.OutputFormat, err)
	}

	if cfg.OutputFile != "" {
		if err := os.WriteFile(cfg.OutputFile, data, 0o644); err != nil {
			return fmt.Errorf("failed to write output to %s: %w", cfg.OutputFile, err)
		}
		fmt.Fprintf(os.Stderr, "Output written to %s\n", cfg.OutputFile)
		return nil
	}

	_, err = os.Stdout.Write(data)
	return err
}
