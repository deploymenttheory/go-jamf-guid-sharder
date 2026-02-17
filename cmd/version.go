package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is set at build time via -ldflags "-X github.com/deploymenttheory/go-jamf-guid-sharder/cmd.Version=x.y.z"
var Version = "dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of go-jamf-guid-sharder",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("go-jamf-guid-sharder version %s\n", Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
