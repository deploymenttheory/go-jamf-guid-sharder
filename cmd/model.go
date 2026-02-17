package cmd

import "time"

// shardConfig represents the complete CLI configuration.
// Field names mirror the jamfpro.ConfigContainer JSON tags so that the same
// config file works with both the SDK and this tool.
type shardConfig struct {
	// Jamf Pro authentication
	InstanceDomain string `mapstructure:"instance_domain"`
	AuthMethod     string `mapstructure:"auth_method"` // "oauth2" or "basic"
	ClientID       string `mapstructure:"client_id"`
	ClientSecret   string `mapstructure:"client_secret"`
	Username       string `mapstructure:"basic_auth_username"`
	Password       string `mapstructure:"basic_auth_password"`

	// HTTP client tuning — mirrors jamfpro.ConfigContainer fields exactly
	LogLevel                    string `mapstructure:"log_level"`
	LogExportPath               string `mapstructure:"log_export_path"`
	HideSensitiveData           bool   `mapstructure:"hide_sensitive_data"`
	JamfLoadBalancerLock        bool   `mapstructure:"jamf_load_balancer_lock"`
	MaxRetryAttempts            int    `mapstructure:"max_retry_attempts"`
	MaxConcurrentRequests       int    `mapstructure:"max_concurrent_requests"`
	EnableDynamicRateLimiting   bool   `mapstructure:"enable_dynamic_rate_limiting"`
	CustomTimeout               int    `mapstructure:"custom_timeout_seconds"`
	TokenRefreshBufferPeriod    int    `mapstructure:"token_refresh_buffer_period_seconds"`
	TotalRetryDuration          int    `mapstructure:"total_retry_duration_seconds"`
	FollowRedirects             bool   `mapstructure:"follow_redirects"`
	MaxRedirects                int    `mapstructure:"max_redirects"`
	EnableConcurrencyManagement bool   `mapstructure:"enable_concurrency_management"`
	MandatoryRequestDelay       int    `mapstructure:"mandatory_request_delay_milliseconds"`
	RetryEligiableRequests      bool   `mapstructure:"retry_eligiable_requests"`

	// Sharding parameters
	SourceType       string              `mapstructure:"source_type"`
	GroupID          string              `mapstructure:"group_id"`
	Strategy         string              `mapstructure:"strategy"`
	ShardCount       int                 `mapstructure:"shard_count"`
	ShardPercentages []int               `mapstructure:"shard_percentages"`
	ShardSizes       []int               `mapstructure:"shard_sizes"`
	Seed             string              `mapstructure:"seed"`
	ExcludeIDs       []string            `mapstructure:"exclude_ids"`
	ReservedIDs      map[string][]string `mapstructure:"reserved_ids"`

	// Output
	OutputFormat string `mapstructure:"output_format"`
	OutputFile   string `mapstructure:"output_file"`
}

// shardReservations holds the separated reserved and unreserved ID lists
// produced during reservation processing.
type shardReservations struct {
	IDsByShard    map[string][]string
	CountsByShard map[int]int
	UnreservedIDs []string
}

// ShardMetadata describes the parameters and statistics of a sharding run.
type ShardMetadata struct {
	GeneratedAt              time.Time `json:"generated_at"                yaml:"generated_at"`
	SourceType               string    `json:"source_type"                 yaml:"source_type"`
	GroupID                  string    `json:"group_id,omitempty"          yaml:"group_id,omitempty"`
	Strategy                 string    `json:"strategy"                    yaml:"strategy"`
	Seed                     string    `json:"seed"                        yaml:"seed"`
	TotalIDsFetched          int       `json:"total_ids_fetched"           yaml:"total_ids_fetched"`
	ExcludedIDCount          int       `json:"excluded_id_count"           yaml:"excluded_id_count"`
	ReservedIDCount          int       `json:"reserved_id_count"           yaml:"reserved_id_count"`
	UnreservedIDsDistributed int       `json:"unreserved_ids_distributed"  yaml:"unreserved_ids_distributed"`
	ShardCount               int       `json:"shard_count"                 yaml:"shard_count"`
}

// ShardResult is the serialisable top-level output of the sharding operation.
type ShardResult struct {
	Metadata ShardMetadata       `json:"metadata" yaml:"metadata"`
	Shards   map[string][]string `json:"shards"   yaml:"shards"`
}
