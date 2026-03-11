package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// ── Exclusions Tests ──────────────────────────────────────────────────────────

func TestApplyExclusions_NoExclusions(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5"}
	result := applyExclusions(ids, []string{})

	assert.Equal(t, ids, result)
}

func TestApplyExclusions_SomeExcluded(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5"}
	excludeIDs := []string{"2", "4"}

	result := applyExclusions(ids, excludeIDs)

	expected := []string{"1", "3", "5"}
	assert.Equal(t, expected, result)
}

func TestApplyExclusions_AllExcluded(t *testing.T) {
	ids := []string{"1", "2", "3"}
	excludeIDs := []string{"1", "2", "3"}

	result := applyExclusions(ids, excludeIDs)

	assert.Empty(t, result)
}

func TestApplyExclusions_NonExistentIDs(t *testing.T) {
	ids := []string{"1", "2", "3"}
	excludeIDs := []string{"10", "20"}

	result := applyExclusions(ids, excludeIDs)

	assert.Equal(t, ids, result)
}

func TestApplyExclusions_EmptyInput(t *testing.T) {
	result := applyExclusions([]string{}, []string{"1", "2"})

	assert.Empty(t, result)
}

// ── Reservations Tests ────────────────────────────────────────────────────────

func TestApplyReservations_NoReservations(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5"}

	result, err := applyReservations(ids, nil, 3)

	require.NoError(t, err)
	assert.Equal(t, ids, result.UnreservedIDs)
	assert.Empty(t, result.IDsByShard)
	assert.Empty(t, result.CountsByShard)
}

func TestApplyReservations_EmptyMap(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5"}

	result, err := applyReservations(ids, map[string][]string{}, 3)

	require.NoError(t, err)
	assert.Equal(t, ids, result.UnreservedIDs)
	assert.Empty(t, result.IDsByShard)
	assert.Empty(t, result.CountsByShard)
}

func TestApplyReservations_ValidReservations(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5", "6", "7", "8"}
	reservedMap := map[string][]string{
		"shard_0": {"1", "2"},
		"shard_2": {"5"},
	}

	result, err := applyReservations(ids, reservedMap, 3)

	require.NoError(t, err)
	assert.Len(t, result.UnreservedIDs, 5)
	assert.Equal(t, []string{"1", "2"}, result.IDsByShard["shard_0"])
	assert.Equal(t, []string{"5"}, result.IDsByShard["shard_2"])
	assert.Equal(t, 2, result.CountsByShard[0])
	assert.Equal(t, 1, result.CountsByShard[2])

	for _, id := range []string{"1", "2", "5"} {
		assert.NotContains(t, result.UnreservedIDs, id)
	}
	for _, id := range []string{"3", "4", "6", "7", "8"} {
		assert.Contains(t, result.UnreservedIDs, id)
	}
}

func TestApplyReservations_InvalidShardName(t *testing.T) {
	ids := []string{"1", "2", "3"}
	reservedMap := map[string][]string{
		"invalid_name": {"1"},
	}

	_, err := applyReservations(ids, reservedMap, 3)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid shard name")
}

func TestApplyReservations_ShardIndexOutOfRange(t *testing.T) {
	ids := []string{"1", "2", "3"}
	reservedMap := map[string][]string{
		"shard_5": {"1"},
	}

	_, err := applyReservations(ids, reservedMap, 3)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestApplyReservations_DuplicateIDInMultipleShards(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5"}
	reservedMap := map[string][]string{
		"shard_0": {"1", "2"},
		"shard_1": {"2", "3"},
	}

	_, err := applyReservations(ids, reservedMap, 3)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "appears in multiple reserved_ids shards")
	assert.Contains(t, err.Error(), "\"2\"")
}

func TestApplyReservations_NegativeShardIndex(t *testing.T) {
	ids := []string{"1", "2", "3"}
	reservedMap := map[string][]string{
		"shard_-1": {"1"},
	}

	_, err := applyReservations(ids, reservedMap, 3)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

// ── Resolve Shard Count Tests ─────────────────────────────────────────────────

func TestResolveShardCount_FromShardCount(t *testing.T) {
	cfg := &shardConfig{
		ShardCount: 5,
	}

	count := resolveShardCount(cfg)

	assert.Equal(t, 5, count)
}

func TestResolveShardCount_FromPercentages(t *testing.T) {
	cfg := &shardConfig{
		ShardPercentages: []int{10, 30, 60},
		ShardCount:       5,
	}

	count := resolveShardCount(cfg)

	assert.Equal(t, 3, count, "Should use percentages length over shard_count")
}

func TestResolveShardCount_FromSizes(t *testing.T) {
	cfg := &shardConfig{
		ShardSizes: []int{10, 20, 30},
		ShardCount: 5,
	}

	count := resolveShardCount(cfg)

	assert.Equal(t, 3, count, "Should use sizes length over shard_count")
}

func TestResolveShardCount_PercentagesPriority(t *testing.T) {
	cfg := &shardConfig{
		ShardPercentages: []int{10, 30, 60},
		ShardSizes:       []int{10, 20},
		ShardCount:       5,
	}

	count := resolveShardCount(cfg)

	assert.Equal(t, 3, count, "Percentages should take priority")
}

// ── Apply Strategy Tests ──────────────────────────────────────────────────────

func TestApplyStrategy_RoundRobin(t *testing.T) {
	cfg := &shardConfig{
		Strategy:   "round-robin",
		ShardCount: 3,
		Seed:       "test",
	}
	ids := createTestIDs(9, 1)

	shards, err := applyStrategy(cfg, ids, &shardReservations{UnreservedIDs: ids})

	require.NoError(t, err)
	assert.Len(t, shards, 3)
}

func TestApplyStrategy_Rendezvous(t *testing.T) {
	cfg := &shardConfig{
		Strategy:   "rendezvous",
		ShardCount: 3,
		Seed:       "test",
	}
	ids := createTestIDs(9, 1)

	shards, err := applyStrategy(cfg, ids, &shardReservations{UnreservedIDs: ids})

	require.NoError(t, err)
	assert.Len(t, shards, 3)
}

func TestApplyStrategy_Percentage(t *testing.T) {
	cfg := &shardConfig{
		Strategy:         "percentage",
		ShardPercentages: []int{10, 30, 60},
		Seed:             "test",
	}
	ids := createTestIDs(100, 1)

	shards, err := applyStrategy(cfg, ids, &shardReservations{UnreservedIDs: ids})

	require.NoError(t, err)
	assert.Len(t, shards, 3)
}

func TestApplyStrategy_Size(t *testing.T) {
	cfg := &shardConfig{
		Strategy:   "size",
		ShardSizes: []int{10, 20, -1},
		Seed:       "test",
	}
	ids := createTestIDs(50, 1)

	shards, err := applyStrategy(cfg, ids, &shardReservations{UnreservedIDs: ids})

	require.NoError(t, err)
	assert.Len(t, shards, 3)
}

func TestApplyStrategy_UnknownStrategy(t *testing.T) {
	cfg := &shardConfig{
		Strategy:   "unknown-strategy",
		ShardCount: 3,
	}
	ids := createTestIDs(9, 1)

	_, err := applyStrategy(cfg, ids, &shardReservations{UnreservedIDs: ids})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown strategy")
}

// ── Output Tests ──────────────────────────────────────────────────────────────

func TestWriteOutput_JSON_Stdout(t *testing.T) {
	cfg := &shardConfig{
		OutputFormat: "json",
		OutputFile:   "",
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:     time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:      "computer_inventory",
			Strategy:        "round-robin",
			Seed:            "test",
			TotalIDsFetched: 10,
			ShardCount:      2,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "3", "5"},
			"shard_1": {"2", "4"},
		},
	}

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := writeOutput(cfg, result)

	w.Close()
	os.Stdout = oldStdout
	r.Close()

	require.NoError(t, err)
}

func TestWriteOutput_JSON_File(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "test_output.json")

	cfg := &shardConfig{
		OutputFormat: "json",
		OutputFile:   outputFile,
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:     time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:      "computer_inventory",
			Strategy:        "round-robin",
			Seed:            "test",
			TotalIDsFetched: 10,
			ShardCount:      2,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "3", "5"},
			"shard_1": {"2", "4"},
		},
	}

	err := writeOutput(cfg, result)

	require.NoError(t, err)
	assert.FileExists(t, outputFile)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var parsed ShardResult
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "computer_inventory", parsed.Metadata.SourceType)
	assert.Equal(t, 2, parsed.Metadata.ShardCount)
	assert.Len(t, parsed.Shards, 2)
}

func TestWriteOutput_YAML_File(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "test_output.yaml")

	cfg := &shardConfig{
		OutputFormat: "yaml",
		OutputFile:   outputFile,
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:     time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:      "mobile_device_inventory",
			Strategy:        "percentage",
			Seed:            "test",
			TotalIDsFetched: 20,
			ShardCount:      3,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "2"},
			"shard_1": {"3", "4", "5"},
			"shard_2": {"6", "7", "8"},
		},
	}

	err := writeOutput(cfg, result)

	require.NoError(t, err)
	assert.FileExists(t, outputFile)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var parsed ShardResult
	err = yaml.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "mobile_device_inventory", parsed.Metadata.SourceType)
	assert.Equal(t, 3, parsed.Metadata.ShardCount)
	assert.Len(t, parsed.Shards, 3)
}

func TestWriteOutput_InvalidPath(t *testing.T) {
	cfg := &shardConfig{
		OutputFormat: "json",
		OutputFile:   "/nonexistent/path/output.json",
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt: time.Now(),
		},
		Shards: map[string][]string{},
	}

	err := writeOutput(cfg, result)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write output")
}

func TestWriteOutput_YAMLStdout(t *testing.T) {
	cfg := &shardConfig{
		OutputFormat: "yaml",
		OutputFile:   "",
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:     time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:      "computer_inventory",
			Strategy:        "round-robin",
			Seed:            "test",
			TotalIDsFetched: 10,
			ShardCount:      2,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "3", "5"},
			"shard_1": {"2", "4"},
		},
	}

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := writeOutput(cfg, result)

	w.Close()
	os.Stdout = oldStdout
	r.Close()

	require.NoError(t, err)
}

func TestWriteOutput_DefaultFormat(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "default_output.json")
	
	cfg := &shardConfig{
		OutputFormat: "",
		OutputFile:   outputFile,
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:     time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:      "computer_inventory",
			Strategy:        "round-robin",
			Seed:            "test",
			TotalIDsFetched: 5,
			ShardCount:      2,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "3"},
			"shard_1": {"2", "4"},
		},
	}

	err := writeOutput(cfg, result)

	require.NoError(t, err)
	assert.FileExists(t, outputFile)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var parsed ShardResult
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err, "Default format should be JSON")
}

func TestWriteOutput_ComplexMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "complex_output.json")
	
	cfg := &shardConfig{
		OutputFormat: "json",
		OutputFile:   outputFile,
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:              time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:               "mobile_device_group_membership",
			Strategy:                 "rendezvous",
			Seed:                     "complex-test",
			TotalIDsFetched:          500,
			ExcludedIDCount:          25,
			ReservedIDCount:          15,
			UnreservedIDsDistributed: 460,
			ShardCount:               5,
		},
		Shards: map[string][]string{
			"shard_0": createTestIDs(92, 1),
			"shard_1": createTestIDs(92, 93),
			"shard_2": createTestIDs(92, 185),
			"shard_3": createTestIDs(92, 277),
			"shard_4": createTestIDs(92, 369),
		},
	}

	err := writeOutput(cfg, result)

	require.NoError(t, err)
	assert.FileExists(t, outputFile)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var parsed ShardResult
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, 500, parsed.Metadata.TotalIDsFetched)
	assert.Equal(t, 25, parsed.Metadata.ExcludedIDCount)
	assert.Equal(t, 15, parsed.Metadata.ReservedIDCount)
	assert.Equal(t, 460, parsed.Metadata.UnreservedIDsDistributed)
}

// ── Fetch Source IDs Tests ────────────────────────────────────────────────────

func TestFetchSourceIDs_UnknownSourceType(t *testing.T) {
	cfg := &shardConfig{
		SourceType: "unknown_source",
	}

	_, err := fetchSourceIDs(nil, cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown source_type")
}


// ── Integration Tests ─────────────────────────────────────────────────────────

func TestFullShardingWorkflow_RoundRobin(t *testing.T) {
	ids := createTestIDs(100, 1)
	cfg := &shardConfig{
		Strategy:   "round-robin",
		ShardCount: 4,
		Seed:       "integration-test",
		ExcludeIDs: []string{"10", "20", "30"},
	}

	filtered := applyExclusions(ids, cfg.ExcludeIDs)
	assert.Len(t, filtered, 97)

	reservations, err := applyReservations(filtered, nil, cfg.ShardCount)
	require.NoError(t, err)

	shards, err := applyStrategy(cfg, filtered, reservations)
	require.NoError(t, err)

	require.Len(t, shards, 4)
	totalDistributed := 0
	for _, shard := range shards {
		totalDistributed += len(shard)
	}
	assert.Equal(t, 97, totalDistributed)
}

func TestFullShardingWorkflow_WithReservations(t *testing.T) {
	ids := createTestIDs(100, 1)
	cfg := &shardConfig{
		Strategy:         "percentage",
		ShardPercentages: []int{20, 30, 50},
		Seed:             "integration-test",
		ExcludeIDs:       []string{"10", "20"},
		ReservedIDs: map[string][]string{
			"shard_0": {"1", "2"},
			"shard_2": {"50"},
		},
	}

	filtered := applyExclusions(ids, cfg.ExcludeIDs)
	assert.Len(t, filtered, 98)

	shardCount := resolveShardCount(cfg)
	assert.Equal(t, 3, shardCount)

	reservations, err := applyReservations(filtered, cfg.ReservedIDs, shardCount)
	require.NoError(t, err)
	assert.Len(t, reservations.UnreservedIDs, 95)

	shards, err := applyStrategy(cfg, filtered, reservations)
	require.NoError(t, err)

	require.Len(t, shards, 3)
	assert.Contains(t, shards[0], "1")
	assert.Contains(t, shards[0], "2")
	assert.Contains(t, shards[2], "50")
}

func TestFullShardingWorkflow_SizeWithRemainder(t *testing.T) {
	ids := createTestIDs(100, 1)
	cfg := &shardConfig{
		Strategy:   "size",
		ShardSizes: []int{20, 30, -1},
		Seed:       "test",
	}

	reservations, err := applyReservations(ids, nil, len(cfg.ShardSizes))
	require.NoError(t, err)

	shards, err := applyStrategy(cfg, ids, reservations)
	require.NoError(t, err)

	require.Len(t, shards, 3)
	assert.Equal(t, 20, len(shards[0]))
	assert.Equal(t, 30, len(shards[1]))
	assert.Equal(t, 50, len(shards[2]), "Remainder shard should get all remaining IDs")
}

func TestFullShardingWorkflow_Rendezvous(t *testing.T) {
	ids := createTestIDs(100, 1)
	cfg := &shardConfig{
		Strategy:   "rendezvous",
		ShardCount: 5,
		Seed:       "stability-test",
	}

	reservations, err := applyReservations(ids, nil, cfg.ShardCount)
	require.NoError(t, err)

	shards, err := applyStrategy(cfg, ids, reservations)
	require.NoError(t, err)

	require.Len(t, shards, 5)
	totalDistributed := 0
	for _, shard := range shards {
		totalDistributed += len(shard)
	}
	assert.Equal(t, 100, totalDistributed)
}

// ── Output Format Tests ───────────────────────────────────────────────────────

func TestWriteOutput_JSONFormat(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.json")

	cfg := &shardConfig{
		OutputFormat: "json",
		OutputFile:   outputFile,
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:              time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:               "computer_inventory",
			Strategy:                 "round-robin",
			Seed:                     "test",
			TotalIDsFetched:          100,
			ExcludedIDCount:          5,
			ReservedIDCount:          3,
			UnreservedIDsDistributed: 92,
			ShardCount:               3,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "4", "7"},
			"shard_1": {"2", "5", "8"},
			"shard_2": {"3", "6", "9"},
		},
	}

	err := writeOutput(cfg, result)
	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var parsed ShardResult
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, result.Metadata.SourceType, parsed.Metadata.SourceType)
	assert.Equal(t, result.Metadata.Strategy, parsed.Metadata.Strategy)
	assert.Equal(t, result.Metadata.TotalIDsFetched, parsed.Metadata.TotalIDsFetched)
	assert.Equal(t, result.Metadata.ShardCount, parsed.Metadata.ShardCount)
	assert.Len(t, parsed.Shards, 3)
}

func TestWriteOutput_YAMLFormat(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.yaml")

	cfg := &shardConfig{
		OutputFormat: "yaml",
		OutputFile:   outputFile,
	}
	result := &ShardResult{
		Metadata: ShardMetadata{
			GeneratedAt:              time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC),
			SourceType:               "user_accounts",
			Strategy:                 "size",
			Seed:                     "yaml-test",
			TotalIDsFetched:          50,
			ExcludedIDCount:          2,
			ReservedIDCount:          1,
			UnreservedIDsDistributed: 47,
			ShardCount:               2,
		},
		Shards: map[string][]string{
			"shard_0": {"1", "2", "3"},
			"shard_1": {"4", "5", "6"},
		},
	}

	err := writeOutput(cfg, result)
	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	var parsed ShardResult
	err = yaml.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, result.Metadata.SourceType, parsed.Metadata.SourceType)
	assert.Equal(t, result.Metadata.Strategy, parsed.Metadata.Strategy)
	assert.Len(t, parsed.Shards, 2)
}

// ── Edge Case Tests ───────────────────────────────────────────────────────────

func TestApplyReservations_AllIDsReserved(t *testing.T) {
	ids := []string{"1", "2", "3", "4"}
	reservedMap := map[string][]string{
		"shard_0": {"1", "2"},
		"shard_1": {"3", "4"},
	}

	result, err := applyReservations(ids, reservedMap, 2)

	require.NoError(t, err)
	assert.Empty(t, result.UnreservedIDs)
	assert.Len(t, result.IDsByShard, 2)
	assert.Equal(t, 2, result.CountsByShard[0])
	assert.Equal(t, 2, result.CountsByShard[1])
}

func TestShardByPercentage_SmallIDSet(t *testing.T) {
	ids := []string{"1", "2", "3"}
	percentages := []int{33, 33, 34}

	shards := shardByPercentage(ids, percentages, "", nil)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 3, totalIDs, "All IDs should be distributed")
}

func TestShardBySize_ExactFit(t *testing.T) {
	ids := createTestIDs(60, 1)
	sizes := []int{20, 20, 20}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 3)
	assert.Equal(t, 20, len(shards[0]))
	assert.Equal(t, 20, len(shards[1]))
	assert.Equal(t, 20, len(shards[2]))
}

func TestShardBySize_MoreSizesThanIDs(t *testing.T) {
	ids := []string{"1", "2", "3"}
	sizes := []int{10, 10, 10, 10}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 4)
	totalIDs := 0
	for _, shard := range shards {
		totalIDs += len(shard)
	}
	assert.Equal(t, 3, totalIDs, "Should only distribute available IDs")
}

func TestSortIDsNumerically_LargeNumbers(t *testing.T) {
	ids := []string{"1000", "50", "500", "5", "5000"}
	sortIDsNumerically(ids)

	expected := []string{"5", "50", "500", "1000", "5000"}
	assert.Equal(t, expected, ids)
}

func TestShuffleIDs_SingleElement(t *testing.T) {
	ids := []string{"42"}
	shuffled := shuffleIDs(ids, "test-seed")

	assert.Equal(t, []string{"42"}, shuffled)
}

func TestShuffleIDs_TwoElements(t *testing.T) {
	ids := []string{"1", "2"}
	shuffled := shuffleIDs(ids, "test-seed")

	assert.Len(t, shuffled, 2)
	assert.Contains(t, shuffled, "1")
	assert.Contains(t, shuffled, "2")
}

func TestCreateSeededRNG_EmptySeed(t *testing.T) {
	rng := createSeededRNG("")

	val1 := rng.Intn(1000)
	val2 := rng.Intn(1000)

	assert.True(t, val1 >= 0 && val1 < 1000)
	assert.True(t, val2 >= 0 && val2 < 1000)
}

func TestSortAndShuffleIfSeed_PreservesAllIDs(t *testing.T) {
	ids := createTestIDs(50, 1)
	result := sortAndShuffleIfSeed(ids, "preserve-test")

	assert.Len(t, result, len(ids))
	for _, id := range ids {
		assert.Contains(t, result, id)
	}
}

// ── Boundary Tests ────────────────────────────────────────────────────────────

func TestShardByRoundRobin_OneID(t *testing.T) {
	ids := []string{"1"}
	shards := shardByRoundRobin(ids, 3, "", nil)

	require.Len(t, shards, 3)
	assert.Len(t, shards[0], 1)
	assert.Empty(t, shards[1])
	assert.Empty(t, shards[2])
}

func TestShardByPercentage_OneID(t *testing.T) {
	ids := []string{"1"}
	percentages := []int{33, 33, 34}

	shards := shardByPercentage(ids, percentages, "", nil)

	require.Len(t, shards, 3)
	totalIDs := 0
	for _, shard := range shards {
		totalIDs += len(shard)
	}
	assert.Equal(t, 1, totalIDs)
}

func TestShardBySize_OneID(t *testing.T) {
	ids := []string{"1"}
	sizes := []int{10, 20, 30}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 3)
	totalIDs := 0
	for _, shard := range shards {
		totalIDs += len(shard)
	}
	assert.Equal(t, 1, totalIDs)
}

func TestShardByRendezvous_OneID(t *testing.T) {
	ids := []string{"1"}
	shards := shardByRendezvous(ids, 3, "test", nil)

	require.Len(t, shards, 3)
	totalIDs := 0
	for _, shard := range shards {
		totalIDs += len(shard)
	}
	assert.Equal(t, 1, totalIDs)
}

func TestApplyReservations_EmptyIDList(t *testing.T) {
	reservedMap := map[string][]string{
		"shard_0": {"1", "2"},
	}

	result, err := applyReservations([]string{}, reservedMap, 2)

	require.NoError(t, err)
	assert.Empty(t, result.UnreservedIDs)
	assert.Len(t, result.IDsByShard, 1)
}

// ── Numeric Sorting Tests ─────────────────────────────────────────────────────

func TestSortIDsNumerically_MixedOrder(t *testing.T) {
	ids := []string{"999", "1", "100", "10", "50"}
	sortIDsNumerically(ids)

	expected := []string{"1", "10", "50", "100", "999"}
	assert.Equal(t, expected, ids)
}

func TestSortIDsNumerically_WithLeadingZeros(t *testing.T) {
	ids := []string{"001", "100", "010", "002"}
	sortIDsNumerically(ids)

	expected := []string{"001", "002", "010", "100"}
	assert.Equal(t, expected, ids)
}

// ── Metadata Generation Tests ─────────────────────────────────────────────────

func TestShardResultMetadata_Calculation(t *testing.T) {
	ids := createTestIDs(100, 1)
	cfg := &shardConfig{
		SourceType:   "computer_inventory",
		Strategy:     "round-robin",
		ShardCount:   3,
		Seed:         "test",
		ExcludeIDs:   []string{"10", "20", "30"},
		ReservedIDs: map[string][]string{
			"shard_0": {"1", "2"},
		},
	}

	filtered := applyExclusions(ids, cfg.ExcludeIDs)
	reservations, err := applyReservations(filtered, cfg.ReservedIDs, cfg.ShardCount)
	require.NoError(t, err)

	shards, err := applyStrategy(cfg, filtered, reservations)
	require.NoError(t, err)

	metadata := ShardMetadata{
		GeneratedAt:              time.Now(),
		SourceType:               cfg.SourceType,
		Strategy:                 cfg.Strategy,
		Seed:                     cfg.Seed,
		TotalIDsFetched:          len(ids),
		ExcludedIDCount:          len(cfg.ExcludeIDs),
		ReservedIDCount:          len(cfg.ReservedIDs["shard_0"]),
		UnreservedIDsDistributed: len(reservations.UnreservedIDs),
		ShardCount:               len(shards),
	}

	assert.Equal(t, 100, metadata.TotalIDsFetched)
	assert.Equal(t, 3, metadata.ExcludedIDCount)
	assert.Equal(t, 2, metadata.ReservedIDCount)
	assert.Equal(t, 95, metadata.UnreservedIDsDistributed)
	assert.Equal(t, 3, metadata.ShardCount)
}

// ── Numeric ID Conversion Tests ───────────────────────────────────────────────

func TestNumericIDConversion_ValidIDs(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected int
	}{
		{"single digit", "5", 5},
		{"double digit", "42", 42},
		{"triple digit", "123", 123},
		{"large number", "99999", 99999},
		{"zero", "0", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := strconv.Atoi(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestNumericIDConversion_InvalidIDs(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"non-numeric", "abc"},
		{"mixed", "123abc"},
		{"special chars", "!@#"},
		{"spaces", "1 2 3"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := strconv.Atoi(tc.input)
			assert.Error(t, err)
		})
	}
}

// ── Additional Integration Tests ──────────────────────────────────────────────

func TestFullWorkflow_AllSourceTypes(t *testing.T) {
	testCases := []struct {
		name       string
		sourceType string
		strategy   string
		shardCount int
		seed       string
	}{
		{"computer_inventory_roundrobin", "computer_inventory", "round-robin", 3, "test1"},
		{"mobile_device_inventory_rendezvous", "mobile_device_inventory", "rendezvous", 4, "test2"},
		{"user_accounts_roundrobin", "user_accounts", "round-robin", 2, "test3"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ids := createTestIDs(50, 1)
			cfg := &shardConfig{
				SourceType: tc.sourceType,
				Strategy:   tc.strategy,
				ShardCount: tc.shardCount,
				Seed:       tc.seed,
			}

			reservations, err := applyReservations(ids, nil, tc.shardCount)
			require.NoError(t, err)

			shards, err := applyStrategy(cfg, ids, reservations)
			require.NoError(t, err)

			require.Len(t, shards, tc.shardCount)
			totalIDs := 0
			for _, shard := range shards {
				totalIDs += len(shard)
			}
			assert.Equal(t, 50, totalIDs)
		})
	}
}

func TestFullWorkflow_ComplexReservationsAndExclusions(t *testing.T) {
	ids := createTestIDs(200, 1)
	cfg := &shardConfig{
		Strategy:         "percentage",
		ShardPercentages: []int{25, 35, 40},
		Seed:             "complex",
		ExcludeIDs:       []string{"5", "10", "15", "20", "25", "30"},
		ReservedIDs: map[string][]string{
			"shard_0": {"1", "2", "3"},
			"shard_1": {"50", "51"},
			"shard_2": {"100"},
		},
	}

	filtered := applyExclusions(ids, cfg.ExcludeIDs)
	assert.Len(t, filtered, 194)

	shardCount := resolveShardCount(cfg)
	reservations, err := applyReservations(filtered, cfg.ReservedIDs, shardCount)
	require.NoError(t, err)
	assert.Len(t, reservations.UnreservedIDs, 188)

	shards, err := applyStrategy(cfg, filtered, reservations)
	require.NoError(t, err)

	require.Len(t, shards, 3)
	assert.Contains(t, shards[0], "1")
	assert.Contains(t, shards[1], "50")
	assert.Contains(t, shards[2], "100")
	
	totalIDs := 0
	for _, shard := range shards {
		totalIDs += len(shard)
	}
	assert.Equal(t, 194, totalIDs)
}

func TestResolveShardCount_AllCombinations(t *testing.T) {
	testCases := []struct {
		name     string
		cfg      *shardConfig
		expected int
	}{
		{
			name:     "only_count",
			cfg:      &shardConfig{ShardCount: 5},
			expected: 5,
		},
		{
			name:     "only_percentages",
			cfg:      &shardConfig{ShardPercentages: []int{20, 30, 50}},
			expected: 3,
		},
		{
			name:     "only_sizes",
			cfg:      &shardConfig{ShardSizes: []int{10, 20, 30, 40}},
			expected: 4,
		},
		{
			name: "percentages_override_count",
			cfg: &shardConfig{
				ShardPercentages: []int{25, 75},
				ShardCount:       10,
			},
			expected: 2,
		},
		{
			name: "sizes_override_count",
			cfg: &shardConfig{
				ShardSizes: []int{10, 20, -1},
				ShardCount: 10,
			},
			expected: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := resolveShardCount(tc.cfg)
			assert.Equal(t, tc.expected, result)
		})
	}
}
