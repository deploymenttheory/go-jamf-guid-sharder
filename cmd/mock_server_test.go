package cmd

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/deploymenttheory/go-sdk-jamfpro-v2/jamfpro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── HTTP Mock Server Helpers ──────────────────────────────────────────────────

func setupMockServer(t *testing.T, handlers map[string]http.HandlerFunc) (*httptest.Server, *jamfpro.Client) {
	mux := http.NewServeMux()
	for path, handler := range handlers {
		mux.HandleFunc(path, handler)
	}

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	authConfig := &jamfpro.AuthConfig{
		InstanceDomain: server.URL,
		AuthMethod:     "oauth2",
		ClientID:       "test-client",
		ClientSecret:   "test-secret",
	}

	client, err := jamfpro.NewClient(authConfig)
	require.NoError(t, err)

	return server, client
}

// ── Fetch Computer Inventory Tests ────────────────────────────────────────────

func TestFetchComputerInventory_Success(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			response := map[string]any{
				"totalCount": 3,
				"results": []map[string]any{
					{
						"id": "1",
						"general": map[string]any{
							"name": "Computer1",
							"remoteManagement": map[string]any{
								"managed": true,
							},
						},
					},
					{
						"id": "2",
						"general": map[string]any{
							"name": "Computer2",
							"remoteManagement": map[string]any{
								"managed": false,
							},
						},
					},
					{
						"id": "3",
						"general": map[string]any{
							"name": "Computer3",
							"remoteManagement": map[string]any{
								"managed": true,
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(response)
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerInventory(client)

	require.NoError(t, err)
	assert.Len(t, ids, 2, "Should only return managed computers")
	assert.Contains(t, ids, "1")
	assert.Contains(t, ids, "3")
	assert.NotContains(t, ids, "2", "Unmanaged computer should be excluded")
}

func TestFetchComputerInventory_AllUnmanaged(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			response := map[string]any{
				"totalCount": 2,
				"results": []map[string]any{
					{
						"id": "1",
						"general": map[string]any{
							"remoteManagement": map[string]any{
								"managed": false,
							},
						},
					},
					{
						"id": "2",
						"general": map[string]any{
							"remoteManagement": map[string]any{
								"managed": false,
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(response)
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerInventory(client)

	require.NoError(t, err)
	assert.Empty(t, ids, "Should return empty list when all computers are unmanaged")
}

func TestFetchComputerInventory_EmptyResponse(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"totalCount": 0,
				"results":    []any{},
			})
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerInventory(client)

	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestFetchComputerInventory_APIError(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal Server Error"))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerInventory(client)

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "failed to retrieve computer inventory")
}

// ── Fetch Mobile Device Inventory Tests ───────────────────────────────────────

func TestFetchMobileDeviceInventory_Success(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevices": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<mobile_devices>
	<size>3</size>
	<mobile_device>
		<id>101</id>
		<name>iPad1</name>
		<managed>true</managed>
	</mobile_device>
	<mobile_device>
		<id>102</id>
		<name>iPad2</name>
		<managed>false</managed>
	</mobile_device>
	<mobile_device>
		<id>103</id>
		<name>iPad3</name>
		<managed>true</managed>
	</mobile_device>
</mobile_devices>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceInventory(client)

	require.NoError(t, err)
	assert.Len(t, ids, 2, "Should only return managed devices")
	assert.Contains(t, ids, "101")
	assert.Contains(t, ids, "103")
	assert.NotContains(t, ids, "102", "Unmanaged device should be excluded")
}

func TestFetchMobileDeviceInventory_EmptyResponse(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevices": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<mobile_devices>
	<size>0</size>
</mobile_devices>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceInventory(client)

	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestFetchMobileDeviceInventory_APIError(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevices": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte("Forbidden"))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceInventory(client)

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "failed to retrieve mobile devices")
}

// ── Fetch Computer Group Members Tests ────────────────────────────────────────

func TestFetchComputerGroupMembers_Success(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/computergroups/id/42": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<computer_group>
	<id>42</id>
	<name>Test Group</name>
	<is_smart>false</is_smart>
	<computers>
		<computer>
			<id>1</id>
			<name>Computer1</name>
		</computer>
		<computer>
			<id>2</id>
			<name>Computer2</name>
		</computer>
		<computer>
			<id>3</id>
			<name>Computer3</name>
		</computer>
	</computers>
</computer_group>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerGroupMembers(client, "42")

	require.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Equal(t, []string{"1", "2", "3"}, ids)
}

func TestFetchComputerGroupMembers_EmptyGroup(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/computergroups/id/42": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<computer_group>
	<id>42</id>
	<name>Empty Group</name>
	<is_smart>false</is_smart>
	<computers/>
</computer_group>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerGroupMembers(client, "42")

	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestFetchComputerGroupMembers_InvalidGroupID(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerGroupMembers(client, "not-a-number")

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "invalid group ID")
	assert.Contains(t, err.Error(), "must be numeric")
}

func TestFetchComputerGroupMembers_APIError(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/computergroups/id/999": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("Group not found"))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerGroupMembers(client, "999")

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "failed to retrieve computer group")
}

// ── Fetch Mobile Device Group Members Tests ───────────────────────────────────

func TestFetchMobileDeviceGroupMembers_Success(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevicegroups/id/50": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<mobile_device_group>
	<id>50</id>
	<name>Test Mobile Group</name>
	<is_smart>false</is_smart>
	<mobile_devices>
		<mobile_device>
			<id>201</id>
			<name>iPad1</name>
		</mobile_device>
		<mobile_device>
			<id>202</id>
			<name>iPad2</name>
		</mobile_device>
	</mobile_devices>
</mobile_device_group>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceGroupMembers(client, "50")

	require.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Equal(t, []string{"201", "202"}, ids)
}

func TestFetchMobileDeviceGroupMembers_EmptyGroup(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevicegroups/id/50": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<mobile_device_group>
	<id>50</id>
	<name>Empty Mobile Group</name>
	<is_smart>false</is_smart>
	<mobile_devices/>
</mobile_device_group>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceGroupMembers(client, "50")

	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestFetchMobileDeviceGroupMembers_InvalidGroupID(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceGroupMembers(client, "invalid-id")

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "invalid group ID")
	assert.Contains(t, err.Error(), "must be numeric")
}

func TestFetchMobileDeviceGroupMembers_APIError(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevicegroups/id/999": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("Group not found"))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceGroupMembers(client, "999")

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "failed to retrieve mobile device group")
}

// ── Fetch Users Tests ─────────────────────────────────────────────────────────

func TestFetchUsers_Success(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/users": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<users>
	<size>4</size>
	<user>
		<id>1001</id>
		<name>user1</name>
	</user>
	<user>
		<id>1002</id>
		<name>user2</name>
	</user>
	<user>
		<id>1003</id>
		<name>user3</name>
	</user>
	<user>
		<id>1004</id>
		<name>user4</name>
	</user>
</users>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchUsers(client)

	require.NoError(t, err)
	assert.Len(t, ids, 4)
	assert.Equal(t, []string{"1001", "1002", "1003", "1004"}, ids)
}

func TestFetchUsers_EmptyResponse(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/users": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<users>
	<size>0</size>
</users>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchUsers(client)

	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestFetchUsers_APIError(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/users": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized"))
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchUsers(client)

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Contains(t, err.Error(), "failed to retrieve users")
}

// ── Fetch Source IDs Integration Tests ────────────────────────────────────────

func TestFetchSourceIDs_ComputerInventory(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			response := map[string]any{
				"totalCount": 2,
				"results": []map[string]any{
					{
						"id": "100",
						"general": map[string]any{
							"remoteManagement": map[string]any{
								"managed": true,
							},
						},
					},
					{
						"id": "101",
						"general": map[string]any{
							"remoteManagement": map[string]any{
								"managed": true,
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(response)
		},
	}

	_, client := setupMockServer(t, handlers)

	cfg := &shardConfig{
		SourceType: "computer_inventory",
	}

	ids, err := fetchSourceIDs(client, cfg)

	require.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, "100")
	assert.Contains(t, ids, "101")
}

func TestFetchSourceIDs_MobileDeviceInventory(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevices": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<mobile_devices>
	<size>2</size>
	<mobile_device>
		<id>200</id>
		<name>iPad1</name>
		<managed>true</managed>
	</mobile_device>
	<mobile_device>
		<id>201</id>
		<name>iPad2</name>
		<managed>true</managed>
	</mobile_device>
</mobile_devices>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	cfg := &shardConfig{
		SourceType: "mobile_device_inventory",
	}

	ids, err := fetchSourceIDs(client, cfg)

	require.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, "200")
	assert.Contains(t, ids, "201")
}

func TestFetchSourceIDs_ComputerGroupMembership(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/computergroups/id/10": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<computer_group>
	<id>10</id>
	<name>Test Group</name>
	<computers>
		<computer><id>5</id></computer>
		<computer><id>6</id></computer>
	</computers>
</computer_group>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	cfg := &shardConfig{
		SourceType: "computer_group_membership",
		GroupID:    "10",
	}

	ids, err := fetchSourceIDs(client, cfg)

	require.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, "5")
	assert.Contains(t, ids, "6")
}

func TestFetchSourceIDs_MobileDeviceGroupMembership(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/mobiledevicegroups/id/20": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<mobile_device_group>
	<id>20</id>
	<name>Test Mobile Group</name>
	<mobile_devices>
		<mobile_device><id>301</id></mobile_device>
		<mobile_device><id>302</id></mobile_device>
		<mobile_device><id>303</id></mobile_device>
	</mobile_devices>
</mobile_device_group>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	cfg := &shardConfig{
		SourceType: "mobile_device_group_membership",
		GroupID:    "20",
	}

	ids, err := fetchSourceIDs(client, cfg)

	require.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Contains(t, ids, "301")
	assert.Contains(t, ids, "302")
	assert.Contains(t, ids, "303")
}

func TestFetchSourceIDs_UserAccounts(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/users": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			response := `<?xml version="1.0" encoding="UTF-8"?>
<users>
	<size>3</size>
	<user>
		<id>401</id>
		<name>user1</name>
	</user>
	<user>
		<id>402</id>
		<name>user2</name>
	</user>
	<user>
		<id>403</id>
		<name>user3</name>
	</user>
</users>`
			w.Write([]byte(response))
		},
	}

	_, client := setupMockServer(t, handlers)

	cfg := &shardConfig{
		SourceType: "user_accounts",
	}

	ids, err := fetchSourceIDs(client, cfg)

	require.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Contains(t, ids, "401")
	assert.Contains(t, ids, "402")
	assert.Contains(t, ids, "403")
}

// ── Additional Edge Cases ─────────────────────────────────────────────────────

func TestFetchComputerInventory_LargeDataset(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/api/v3/computers-inventory": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			results := make([]map[string]any, 100)
			for i := range 100 {
				results[i] = map[string]any{
					"id": fmt.Sprintf("%d", i+1),
					"general": map[string]any{
						"remoteManagement": map[string]any{
							"managed": true,
						},
					},
				}
			}

			response := map[string]any{
				"totalCount": 100,
				"results":    results,
			}
			json.NewEncoder(w).Encode(response)
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerInventory(client)

	require.NoError(t, err)
	assert.Len(t, ids, 100)
	assert.Contains(t, ids, "1")
	assert.Contains(t, ids, "50")
	assert.Contains(t, ids, "100")
}

func TestFetchMobileDeviceInventory_MixedManagedStatus(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
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
				Size: 10,
				Devices: []MobileDevice{
					{ID: 1, Name: "Device1", Managed: true},
					{ID: 2, Name: "Device2", Managed: false},
					{ID: 3, Name: "Device3", Managed: true},
					{ID: 4, Name: "Device4", Managed: false},
					{ID: 5, Name: "Device5", Managed: true},
					{ID: 6, Name: "Device6", Managed: false},
					{ID: 7, Name: "Device7", Managed: true},
					{ID: 8, Name: "Device8", Managed: false},
					{ID: 9, Name: "Device9", Managed: true},
					{ID: 10, Name: "Device10", Managed: true},
				},
			}

			w.Header().Set("Content-Type", "application/xml")
			xml.NewEncoder(w).Encode(devices)
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchMobileDeviceInventory(client)

	require.NoError(t, err)
	assert.Len(t, ids, 6, "Should only return 6 managed devices out of 10")
	assert.Contains(t, ids, "1")
	assert.Contains(t, ids, "3")
	assert.Contains(t, ids, "5")
	assert.Contains(t, ids, "7")
	assert.Contains(t, ids, "9")
	assert.Contains(t, ids, "10")
	assert.NotContains(t, ids, "2")
	assert.NotContains(t, ids, "4")
	assert.NotContains(t, ids, "6")
	assert.NotContains(t, ids, "8")
}

func TestFetchComputerGroupMembers_LargeGroup(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"/api/v1/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-token",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		},
		"/JSSResource/computergroups/id/100": func(w http.ResponseWriter, r *http.Request) {
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

			computers := make([]Computer, 50)
			for i := range 50 {
				computers[i] = Computer{
					ID:   i + 1,
					Name: fmt.Sprintf("Computer%d", i+1),
				}
			}

			group := ComputerGroup{
				ID:        100,
				Name:      "Large Group",
				IsSmart:   false,
				Computers: computers,
			}

			w.Header().Set("Content-Type", "application/xml")
			xml.NewEncoder(w).Encode(group)
		},
	}

	_, client := setupMockServer(t, handlers)

	ids, err := fetchComputerGroupMembers(client, "100")

	require.NoError(t, err)
	assert.Len(t, ids, 50)
	assert.Contains(t, ids, "1")
	assert.Contains(t, ids, "25")
	assert.Contains(t, ids, "50")
}
