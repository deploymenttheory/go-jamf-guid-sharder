package cmd

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Integration Tests for runShard ────────────────────────────────────────────

func setupIntegrationTest(t *testing.T) (*httptest.Server, func()) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v1/auth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"token":   "mock-basic-token",
				"expires": "2026-03-11T23:59:59Z",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			results := make([]map[string]any, 50)
			for i := range 50 {
				results[i] = map[string]any{
					"id": fmt.Sprintf("%d", i+1),
					"general": map[string]any{
						"name": fmt.Sprintf("Computer%d", i+1),
						"remoteManagement": map[string]any{
							"managed": true,
						},
					},
				}
			}

			response := map[string]any{
				"totalCount": 50,
				"results":    results,
			}
			json.NewEncoder(w).Encode(response)
		},
		"/JSSResource/mobiledevices": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")

			type MobileDevice struct {
				XMLName xml.Name `xml:"mobile_device"`
				ID      int      `xml:"id"`
				Name    string   `xml:"name"`
				Managed bool     `xml:"managed"`
			}

			type MobileDevices struct {
				XMLName xml.Name       `xml:"mobile_devices"`
				Size    int            `xml:"size"`
				Devices []MobileDevice `xml:"mobile_device"`
			}

			devices := MobileDevices{
				Size: 30,
			}
			for i := range 30 {
				devices.Devices = append(devices.Devices, MobileDevice{
					ID:      i + 100,
					Name:    fmt.Sprintf("iPad%d", i+1),
					Managed: true,
				})
			}

			xml.NewEncoder(w).Encode(devices)
		},
		"/JSSResource/users": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")

			type User struct {
				XMLName xml.Name `xml:"user"`
				ID      int      `xml:"id"`
				Name    string   `xml:"name"`
			}

			type Users struct {
				XMLName xml.Name `xml:"users"`
				Size    int      `xml:"size"`
				Users   []User   `xml:"user"`
			}

			users := Users{
				Size: 20,
			}
			for i := range 20 {
				users.Users = append(users.Users, User{
					ID:   i + 1000,
					Name: fmt.Sprintf("user%d", i+1),
				})
			}

			xml.NewEncoder(w).Encode(users)
		},
		"/JSSResource/computergroups/id/10": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")

			type Computer struct {
				XMLName xml.Name `xml:"computer"`
				ID      int      `xml:"id"`
				Name    string   `xml:"name"`
			}

			type ComputerGroup struct {
				XMLName   xml.Name   `xml:"computer_group"`
				ID        int        `xml:"id"`
				Name      string     `xml:"name"`
				IsSmart   bool       `xml:"is_smart"`
				Computers []Computer `xml:"computers>computer"`
			}

			computers := make([]Computer, 25)
			for i := range 25 {
				computers[i] = Computer{
					ID:   i + 1,
					Name: fmt.Sprintf("Computer%d", i+1),
				}
			}

			group := ComputerGroup{
				ID:        10,
				Name:      "Test Group",
				IsSmart:   false,
				Computers: computers,
			}

			xml.NewEncoder(w).Encode(group)
		},
		"/JSSResource/mobiledevicegroups/id/20": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")

			type MobileDevice struct {
				XMLName xml.Name `xml:"mobile_device"`
				ID      int      `xml:"id"`
				Name    string   `xml:"name"`
			}

			type MobileDeviceGroup struct {
				XMLName       xml.Name       `xml:"mobile_device_group"`
				ID            int            `xml:"id"`
				Name          string         `xml:"name"`
				IsSmart       bool           `xml:"is_smart"`
				MobileDevices []MobileDevice `xml:"mobile_devices>mobile_device"`
			}

			devices := make([]MobileDevice, 15)
			for i := range 15 {
				devices[i] = MobileDevice{
					ID:   i + 200,
					Name: fmt.Sprintf("iPad%d", i+1),
				}
			}

			group := MobileDeviceGroup{
				ID:            20,
				Name:          "Test Mobile Group",
				IsSmart:       false,
				MobileDevices: devices,
			}

			xml.NewEncoder(w).Encode(group)
		},
	}

	mux := http.NewServeMux()
	for path, handler := range handlers {
		mux.HandleFunc(path, handler)
	}

	server := httptest.NewServer(mux)

	cleanup := func() {
		server.Close()
		viper.Reset()
	}

	return server, cleanup
}

func TestRunShard_ComputerInventory_RoundRobin(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "round-robin")
	viper.Set("shard_count", 3)
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)
	viper.Set("seed", "integration-test")

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)
	assert.FileExists(t, outputFile)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "computer_inventory", result.Metadata.SourceType)
	assert.Equal(t, "round-robin", result.Metadata.Strategy)
	assert.Equal(t, 50, result.Metadata.TotalIDsFetched)
	assert.Equal(t, 3, result.Metadata.ShardCount)
	assert.Len(t, result.Shards, 3)
}

func TestRunShard_MobileDeviceInventory_Rendezvous(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.yaml")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "mobile_device_inventory")
	viper.Set("strategy", "rendezvous")
	viper.Set("shard_count", 4)
	viper.Set("output_format", "yaml")
	viper.Set("output_file", outputFile)
	viper.Set("seed", "rendezvous-test")

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)
	assert.FileExists(t, outputFile)
}

func TestRunShard_UserAccounts_Percentage(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "user_accounts")
	viper.Set("strategy", "percentage")
	viper.Set("shard_percentages", []int{20, 30, 50})
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)
	assert.FileExists(t, outputFile)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "user_accounts", result.Metadata.SourceType)
	assert.Equal(t, "percentage", result.Metadata.Strategy)
	assert.Equal(t, 20, result.Metadata.TotalIDsFetched)
}

func TestRunShard_WithExclusions(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "round-robin")
	viper.Set("shard_count", 3)
	viper.Set("exclude_ids", []string{"5", "10", "15"})
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, 50, result.Metadata.TotalIDsFetched)
	assert.Equal(t, 3, result.Metadata.ExcludedIDCount)
	assert.Equal(t, 47, result.Metadata.UnreservedIDsDistributed)
}

func TestRunShard_WithReservations(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "round-robin")
	viper.Set("shard_count", 3)
	viper.Set("reserved_ids", map[string][]string{
		"shard_0": {"1", "2", "3"},
		"shard_2": {"10"},
	})
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, 4, result.Metadata.ReservedIDCount)
	assert.Contains(t, result.Shards["shard_0"], "1")
	assert.Contains(t, result.Shards["shard_2"], "10")
}

func TestRunShard_WithReservationsFromFlag(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "round-robin")
	viper.Set("shard_count", 2)
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")
	cmd.Flags().Set("reserved-ids", `{"shard_0":["1","2"],"shard_1":["5"]}`)

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Metadata.ReservedIDCount)
	assert.Contains(t, result.Shards["shard_0"], "1")
	assert.Contains(t, result.Shards["shard_1"], "5")
}

func TestRunShard_InvalidReservedIDsJSON(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "round-robin")
	viper.Set("shard_count", 2)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")
	cmd.Flags().Set("reserved-ids", `{invalid json}`)

	err := runShard(cmd, []string{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid --reserved-ids JSON")
}

func TestRunShard_ComputerGroupMembership(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_group_membership")
	viper.Set("group_id", "10")
	viper.Set("strategy", "size")
	viper.Set("shard_sizes", []int{10, 15})
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "computer_group_membership", result.Metadata.SourceType)
	assert.Equal(t, "10", result.Metadata.GroupID)
	assert.Equal(t, 25, result.Metadata.TotalIDsFetched)
}

func TestRunShard_MobileDeviceGroupMembership(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "mobile_device_group_membership")
	viper.Set("group_id", "20")
	viper.Set("strategy", "round-robin")
	viper.Set("shard_count", 3)
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "mobile_device_group_membership", result.Metadata.SourceType)
	assert.Equal(t, "20", result.Metadata.GroupID)
	assert.Equal(t, 15, result.Metadata.TotalIDsFetched)
}

func TestRunShard_ValidationFailure(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "invalid-strategy")
	viper.Set("shard_count", 3)

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}

func TestRunShard_SizeStrategy(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "user_accounts")
	viper.Set("strategy", "size")
	viper.Set("shard_sizes", []int{5, 10, -1})
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)
	viper.Set("seed", "size-test")

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "size", result.Metadata.Strategy)
	assert.Equal(t, 20, result.Metadata.TotalIDsFetched)
	assert.Len(t, result.Shards, 3)
	assert.Equal(t, 5, len(result.Shards["shard_0"]))
	assert.Equal(t, 10, len(result.Shards["shard_1"]))
	assert.Equal(t, 5, len(result.Shards["shard_2"]))
}

func TestRunShard_ComplexWorkflow(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	viper.Set("instance_domain", server.URL)
	viper.Set("auth_method", "oauth2")
	viper.Set("client_id", "test-client")
	viper.Set("client_secret", "test-secret")
	viper.Set("source_type", "computer_inventory")
	viper.Set("strategy", "percentage")
	viper.Set("shard_percentages", []int{30, 40, 30})
	viper.Set("exclude_ids", []string{"5", "10", "15", "20"})
	viper.Set("reserved_ids", map[string][]string{
		"shard_0": {"1", "2"},
		"shard_2": {"25"},
	})
	viper.Set("output_format", "json")
	viper.Set("output_file", outputFile)
	viper.Set("seed", "complex-workflow")

	cmd := &cobra.Command{}
	cmd.Flags().String("reserved-ids", "", "")

	err := runShard(cmd, []string{})

	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var result ShardResult
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, 50, result.Metadata.TotalIDsFetched)
	assert.Equal(t, 4, result.Metadata.ExcludedIDCount)
	assert.Equal(t, 3, result.Metadata.ReservedIDCount)
	assert.Contains(t, result.Shards["shard_0"], "1")
	assert.Contains(t, result.Shards["shard_2"], "25")
}

func TestBuildJamfClient_Integration(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	cfg := &shardConfig{
		InstanceDomain:           server.URL,
		AuthMethod:               "oauth2",
		ClientID:                 "test-client",
		ClientSecret:             "test-secret",
		TokenRefreshBufferPeriod: 300,
		HideSensitiveData:        true,
		CustomTimeout:            60,
		MaxRetryAttempts:         3,
		MaxConcurrentRequests:    5,
		MandatoryRequestDelay:    100,
		TotalRetryDuration:       120,
	}

	client, err := buildJamfClient(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
	assert.NotNil(t, client.JamfProAPI)
	assert.NotNil(t, client.ClassicAPI)
}

func TestBuildJamfClient_BasicAuth(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	cfg := &shardConfig{
		InstanceDomain: server.URL,
		AuthMethod:     "basic",
		Username:       "admin",
		Password:       "password",
	}

	client, err := buildJamfClient(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestBuildJamfClient_MinimalOptions(t *testing.T) {
	server, cleanup := setupIntegrationTest(t)
	defer cleanup()

	cfg := &shardConfig{
		InstanceDomain: server.URL,
		AuthMethod:     "oauth2",
		ClientID:       "test-client",
		ClientSecret:   "test-secret",
	}

	client, err := buildJamfClient(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
}
