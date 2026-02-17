package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "go-jamf-guid-sharder",
	Short: "Shard Jamf Pro device and user IDs into configurable groups",
	Long: `go-jamf-guid-sharder retrieves managed device and user IDs from Jamf Pro
and distributes them into configurable shards for progressive rollouts
and phased deployments.

Supported sharding strategies:
  round-robin   Equal distribution (±1 ID variance)
  percentage    Proportional distribution by specified percentages
  size          Fixed absolute shard sizes with optional remainder (-1)
  rendezvous    Highest Random Weight (HRW) consistent hashing — minimal
                disruption when shard count changes

Configuration can be supplied via:
  1. A config file (YAML or JSON) — default: ./go-jamf-guid-sharder.yaml
  2. Environment variables prefixed with JAMF_  (e.g. JAMF_INSTANCE_DOMAIN)
  3. Command-line flags

Output is written as JSON or YAML to stdout or a file.`,
}

// Execute is the entry point called from main.
// Cobra prints the error itself; we only need to set the exit code.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file path (default: ./go-jamf-guid-sharder.yaml)")
	// Don't reprint the full usage block on every validation error — the error
	// message itself is already actionable. Users can run --help explicitly.
	rootCmd.SilenceUsage = true
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName("go-jamf-guid-sharder")
		viper.SetConfigType("yaml")
	}

	// Environment variable support: JAMF_INSTANCE_DOMAIN, JAMF_CLIENT_ID, etc.
	viper.SetEnvPrefix("JAMF")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
