package cmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper to create sample IDs
func createTestIDs(count int, start int) []string {
	ids := make([]string, count)
	for i := range count {
		ids[i] = fmt.Sprintf("%d", start+i)
	}
	return ids
}

// ── Round-Robin Tests ────────────────────────────────────────────────────────

func TestShardByRoundRobin_EqualDistribution(t *testing.T) {
	ids := createTestIDs(9, 1)
	shards := shardByRoundRobin(ids, 3, "", nil)

	require.Len(t, shards, 3)
	assert.Len(t, shards[0], 3)
	assert.Len(t, shards[1], 3)
	assert.Len(t, shards[2], 3)
}

func TestShardByRoundRobin_UnevenDistribution(t *testing.T) {
	ids := createTestIDs(10, 1)
	shards := shardByRoundRobin(ids, 3, "", nil)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 10, totalIDs)
	assert.True(t, len(shards[0]) >= 3 && len(shards[0]) <= 4)
	assert.True(t, len(shards[1]) >= 3 && len(shards[1]) <= 4)
	assert.True(t, len(shards[2]) >= 3 && len(shards[2]) <= 4)
}

func TestShardByRoundRobin_WithSeed(t *testing.T) {
	ids := createTestIDs(9, 1)

	shards1 := shardByRoundRobin(ids, 3, "test-seed", nil)
	shards2 := shardByRoundRobin(ids, 3, "test-seed", nil)

	require.Len(t, shards1, 3)
	require.Len(t, shards2, 3)

	for i := range 3 {
		assert.Equal(t, shards1[i], shards2[i], "Same seed should produce identical distribution")
	}
}

func TestShardByRoundRobin_DifferentSeeds(t *testing.T) {
	ids := createTestIDs(9, 1)

	shards1 := shardByRoundRobin(ids, 3, "seed1", nil)
	shards2 := shardByRoundRobin(ids, 3, "seed2", nil)

	require.Len(t, shards1, 3)
	require.Len(t, shards2, 3)

	different := false
	for i := range 3 {
		if !slicesEqual(shards1[i], shards2[i]) {
			different = true
			break
		}
	}
	assert.True(t, different, "Different seeds should produce different distributions")
}

func TestShardByRoundRobin_WithReservations(t *testing.T) {
	ids := createTestIDs(10, 1)
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": {"100", "101"},
			"shard_2": {"200"},
		},
		CountsByShard: map[int]int{
			0: 2,
			2: 1,
		},
		UnreservedIDs: ids,
	}

	shards := shardByRoundRobin(ids, 3, "", reservations)

	require.Len(t, shards, 3)
	assert.Contains(t, shards[0], "100")
	assert.Contains(t, shards[0], "101")
	assert.Contains(t, shards[2], "200")
}

func TestShardByRoundRobin_ZeroShardCount(t *testing.T) {
	ids := createTestIDs(5, 1)
	shards := shardByRoundRobin(ids, 0, "", nil)

	require.Len(t, shards, 1)
	assert.Len(t, shards[0], 5)
}

func TestShardByRoundRobin_EmptyIDs(t *testing.T) {
	shards := shardByRoundRobin([]string{}, 3, "", nil)

	require.Len(t, shards, 3)
	for i := range 3 {
		assert.Empty(t, shards[i])
	}
}

// ── Percentage Tests ──────────────────────────────────────────────────────────

func TestShardByPercentage_BasicDistribution(t *testing.T) {
	ids := createTestIDs(100, 1)
	percentages := []int{10, 30, 60}

	shards := shardByPercentage(ids, percentages, "", nil)

	require.Len(t, shards, 3)
	assert.Equal(t, 10, len(shards[0]))
	assert.Equal(t, 30, len(shards[1]))
	assert.Equal(t, 60, len(shards[2]))
}

func TestShardByPercentage_WithRemainder(t *testing.T) {
	ids := createTestIDs(103, 1)
	percentages := []int{10, 30, 60}

	shards := shardByPercentage(ids, percentages, "", nil)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 103, totalIDs, "All IDs should be distributed")
	assert.Equal(t, 103-len(shards[0])-len(shards[1]), len(shards[2]), "Last shard gets remainder")
}

func TestShardByPercentage_WithSeed(t *testing.T) {
	ids := createTestIDs(100, 1)
	percentages := []int{10, 30, 60}

	shards1 := shardByPercentage(ids, percentages, "test-seed", nil)
	shards2 := shardByPercentage(ids, percentages, "test-seed", nil)

	require.Len(t, shards1, 3)
	require.Len(t, shards2, 3)

	for i := range 3 {
		assert.Equal(t, shards1[i], shards2[i], "Same seed should produce identical distribution")
	}
}

func TestShardByPercentage_WithReservations(t *testing.T) {
	ids := createTestIDs(100, 1)
	percentages := []int{10, 30, 60}
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": {"1000", "1001"},
		},
		CountsByShard: map[int]int{
			0: 2,
		},
		UnreservedIDs: ids,
	}

	shards := shardByPercentage(ids, percentages, "", reservations)

	require.Len(t, shards, 3)
	assert.Contains(t, shards[0], "1000")
	assert.Contains(t, shards[0], "1001")
	assert.Equal(t, 10, len(shards[0]), "Shard 0 should have 10 total (8 distributed + 2 reserved)")
}

func TestShardByPercentage_EmptyIDs(t *testing.T) {
	percentages := []int{10, 30, 60}
	shards := shardByPercentage([]string{}, percentages, "", nil)

	require.Len(t, shards, 3)
	for i := range 3 {
		assert.Empty(t, shards[i])
	}
}

func TestShardByPercentage_WithReservationsExceedingTarget(t *testing.T) {
	allIDs := createTestIDs(100, 1)
	reservedIDs := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}
	
	unreservedIDs := make([]string, 0, len(allIDs)-len(reservedIDs))
	reservedSet := make(map[string]bool)
	for _, id := range reservedIDs {
		reservedSet[id] = true
	}
	for _, id := range allIDs {
		if !reservedSet[id] {
			unreservedIDs = append(unreservedIDs, id)
		}
	}
	
	percentages := []int{10, 30, 60}
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": reservedIDs,
		},
		CountsByShard: map[int]int{
			0: 12,
		},
		UnreservedIDs: unreservedIDs,
	}

	shards := shardByPercentage(allIDs, percentages, "", reservations)

	require.Len(t, shards, 3)
	assert.GreaterOrEqual(t, len(shards[0]), 10, "Shard 0 should have at least target percentage")
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 100, totalIDs, "Should have all 100 IDs distributed")
}

func TestShardByPercentage_EdgeCaseRounding(t *testing.T) {
	ids := createTestIDs(97, 1)
	percentages := []int{33, 33, 34}
	
	shards := shardByPercentage(ids, percentages, "", nil)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 97, totalIDs, "All IDs should be distributed")
}

func TestShardByPercentage_ReservationsWithBoundaryCondition(t *testing.T) {
	ids := createTestIDs(50, 1)
	percentages := []int{40, 40, 20}
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": {"100", "101"},
			"shard_1": {"200", "201", "202"},
		},
		CountsByShard: map[int]int{
			0: 2,
			1: 3,
		},
		UnreservedIDs: ids,
	}

	shards := shardByPercentage(ids, percentages, "", reservations)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 55, totalIDs, "Should have 50 unreserved + 5 reserved")
	assert.Contains(t, shards[0], "100")
	assert.Contains(t, shards[1], "200")
}

func TestShardByPercentage_BoundaryOverflow(t *testing.T) {
	ids := createTestIDs(10, 1)
	percentages := []int{50, 50}
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": {"100", "101", "102", "103", "104", "105", "106"},
		},
		CountsByShard: map[int]int{
			0: 7,
		},
		UnreservedIDs: ids,
	}

	shards := shardByPercentage(ids, percentages, "", reservations)

	require.Len(t, shards, 2)
	totalIDs := len(shards[0]) + len(shards[1])
	assert.Equal(t, 17, totalIDs, "Should have 10 unreserved + 7 reserved")
}

// ── Size Tests ────────────────────────────────────────────────────────────────

func TestShardBySize_ExactSizes(t *testing.T) {
	ids := createTestIDs(100, 1)
	sizes := []int{10, 30, 60}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 3)
	assert.Equal(t, 10, len(shards[0]))
	assert.Equal(t, 30, len(shards[1]))
	assert.Equal(t, 60, len(shards[2]))
}

func TestShardBySize_WithRemainder(t *testing.T) {
	ids := createTestIDs(50, 1)
	sizes := []int{10, 20, -1}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 3)
	assert.Equal(t, 10, len(shards[0]))
	assert.Equal(t, 20, len(shards[1]))
	assert.Equal(t, 20, len(shards[2]), "Last shard with -1 should get remaining 20 IDs")
}

func TestShardBySize_WithSeed(t *testing.T) {
	ids := createTestIDs(50, 1)
	sizes := []int{10, 20, 20}

	shards1 := shardBySize(ids, sizes, "test-seed", nil)
	shards2 := shardBySize(ids, sizes, "test-seed", nil)

	require.Len(t, shards1, 3)
	require.Len(t, shards2, 3)

	for i := range 3 {
		assert.Equal(t, shards1[i], shards2[i], "Same seed should produce identical distribution")
	}
}

func TestShardBySize_WithReservations(t *testing.T) {
	ids := createTestIDs(100, 1)
	sizes := []int{10, 30, 60}
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": {"1000", "1001"},
		},
		CountsByShard: map[int]int{
			0: 2,
		},
		UnreservedIDs: ids,
	}

	shards := shardBySize(ids, sizes, "", reservations)

	require.Len(t, shards, 3)
	assert.Contains(t, shards[0], "1000")
	assert.Contains(t, shards[0], "1001")
	assert.Equal(t, 10, len(shards[0]), "Shard 0 should have 10 total (8 distributed + 2 reserved)")
}

func TestShardBySize_InsufficientIDs(t *testing.T) {
	ids := createTestIDs(5, 1)
	sizes := []int{10, 20, 30}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 5, totalIDs, "Should distribute all available IDs")
	assert.Equal(t, 5, len(shards[0]))
	assert.Empty(t, shards[1])
	assert.Empty(t, shards[2])
}

func TestShardBySize_EmptyIDs(t *testing.T) {
	sizes := []int{10, 20, 30}
	shards := shardBySize([]string{}, sizes, "", nil)

	require.Len(t, shards, 3)
	for i := range 3 {
		assert.Empty(t, shards[i])
	}
}

func TestShardBySize_WithReservationsExceedingTarget(t *testing.T) {
	allIDs := createTestIDs(100, 1)
	reservedIDs := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}
	
	unreservedIDs := make([]string, 0, len(allIDs)-len(reservedIDs))
	reservedSet := make(map[string]bool)
	for _, id := range reservedIDs {
		reservedSet[id] = true
	}
	for _, id := range allIDs {
		if !reservedSet[id] {
			unreservedIDs = append(unreservedIDs, id)
		}
	}
	
	sizes := []int{10, 30, 60}
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": reservedIDs,
		},
		CountsByShard: map[int]int{
			0: 12,
		},
		UnreservedIDs: unreservedIDs,
	}

	shards := shardBySize(allIDs, sizes, "", reservations)

	require.Len(t, shards, 3)
	assert.Equal(t, 12, len(shards[0]), "Shard 0 should have 12 reserved IDs (no additional distribution when reserved exceeds target)")
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 100, totalIDs, "Should have all 100 IDs distributed")
}

// ── Rendezvous Tests ──────────────────────────────────────────────────────────

func TestShardByRendezvous_BasicDistribution(t *testing.T) {
	ids := createTestIDs(100, 1)

	shards := shardByRendezvous(ids, 3, "test-seed", nil)

	require.Len(t, shards, 3)
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 100, totalIDs, "All IDs should be distributed")
}

func TestShardByRendezvous_Deterministic(t *testing.T) {
	ids := createTestIDs(50, 1)

	shards1 := shardByRendezvous(ids, 3, "test-seed", nil)
	shards2 := shardByRendezvous(ids, 3, "test-seed", nil)

	require.Len(t, shards1, 3)
	require.Len(t, shards2, 3)

	for i := range 3 {
		assert.Equal(t, shards1[i], shards2[i], "Same seed should produce identical distribution")
	}
}

func TestShardByRendezvous_DifferentSeeds(t *testing.T) {
	ids := createTestIDs(50, 1)

	shards1 := shardByRendezvous(ids, 3, "seed1", nil)
	shards2 := shardByRendezvous(ids, 3, "seed2", nil)

	require.Len(t, shards1, 3)
	require.Len(t, shards2, 3)

	different := false
	for i := range 3 {
		if !slicesEqual(shards1[i], shards2[i]) {
			different = true
			break
		}
	}
	assert.True(t, different, "Different seeds should produce different distributions")
}

func TestShardByRendezvous_WithReservations(t *testing.T) {
	ids := createTestIDs(50, 1)
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_1": {"1000", "1001"},
		},
		CountsByShard: map[int]int{
			1: 2,
		},
		UnreservedIDs: ids,
	}

	shards := shardByRendezvous(ids, 3, "test-seed", reservations)

	require.Len(t, shards, 3)
	assert.Contains(t, shards[1], "1000")
	assert.Contains(t, shards[1], "1001")
	totalIDs := len(shards[0]) + len(shards[1]) + len(shards[2])
	assert.Equal(t, 52, totalIDs, "Should have 50 distributed + 2 reserved")
}

func TestShardByRendezvous_ZeroShardCount(t *testing.T) {
	ids := createTestIDs(10, 1)
	shards := shardByRendezvous(ids, 0, "test-seed", nil)

	require.Len(t, shards, 1)
	assert.Len(t, shards[0], 10)
}

func TestShardByRendezvous_EmptyIDs(t *testing.T) {
	shards := shardByRendezvous([]string{}, 3, "test-seed", nil)

	require.Len(t, shards, 3)
	for i := range 3 {
		assert.Empty(t, shards[i])
	}
}

func TestShardByRendezvous_Stability(t *testing.T) {
	ids := createTestIDs(100, 1)

	shards3 := shardByRendezvous(ids, 3, "stability-test", nil)
	shards4 := shardByRendezvous(ids, 4, "stability-test", nil)

	require.Len(t, shards3, 3)
	require.Len(t, shards4, 4)

	movedCount := 0
	for _, id := range ids {
		inShard3 := findIDShard(id, shards3)
		inShard4 := findIDShard(id, shards4)
		if inShard3 != inShard4 {
			movedCount++
		}
	}

	expectedMoved := 100 / 4
	tolerance := 15
	assert.InDelta(t, expectedMoved, movedCount, float64(tolerance),
		"Rendezvous should move approximately 1/n IDs when shard count changes")
}

// ── Helper Function Tests ─────────────────────────────────────────────────────

func TestSortAndShuffleIfSeed_NoSeed(t *testing.T) {
	ids := []string{"5", "2", "8", "1", "3"}
	result := sortAndShuffleIfSeed(ids, "")

	assert.Equal(t, ids, result, "Without seed, should return original order")
}

func TestSortAndShuffleIfSeed_WithSeed(t *testing.T) {
	ids := []string{"5", "2", "8", "1", "3"}
	result := sortAndShuffleIfSeed(ids, "test-seed")

	assert.Len(t, result, 5)
	assert.NotEqual(t, []string{"1", "2", "3", "5", "8"}, result, "Should be shuffled, not just sorted")

	result2 := sortAndShuffleIfSeed(ids, "test-seed")
	assert.Equal(t, result, result2, "Same seed should produce same shuffle")
}

func TestShuffleIDs_Deterministic(t *testing.T) {
	ids := createTestIDs(20, 1)

	shuffled1 := shuffleIDs(ids, "test-seed")
	shuffled2 := shuffleIDs(ids, "test-seed")

	assert.Equal(t, shuffled1, shuffled2, "Same seed should produce identical shuffle")
	assert.NotEqual(t, ids, shuffled1, "Should be shuffled")
}

func TestShuffleIDs_DifferentSeeds(t *testing.T) {
	ids := createTestIDs(20, 1)

	shuffled1 := shuffleIDs(ids, "seed1")
	shuffled2 := shuffleIDs(ids, "seed2")

	assert.NotEqual(t, shuffled1, shuffled2, "Different seeds should produce different shuffles")
}

func TestShuffleIDs_PreservesAllElements(t *testing.T) {
	ids := createTestIDs(50, 1)
	shuffled := shuffleIDs(ids, "test-seed")

	assert.Len(t, shuffled, len(ids))
	for _, id := range ids {
		assert.Contains(t, shuffled, id, "All original IDs should be present")
	}
}

func TestCreateSeededRNG_Deterministic(t *testing.T) {
	rng1 := createSeededRNG("test-seed")
	rng2 := createSeededRNG("test-seed")

	values1 := make([]int, 10)
	values2 := make([]int, 10)

	for i := range 10 {
		values1[i] = rng1.Intn(1000)
		values2[i] = rng2.Intn(1000)
	}

	assert.Equal(t, values1, values2, "Same seed should produce same random sequence")
}

func TestCreateSeededRNG_DifferentSeeds(t *testing.T) {
	rng1 := createSeededRNG("seed1")
	rng2 := createSeededRNG("seed2")

	values1 := make([]int, 10)
	values2 := make([]int, 10)

	for i := range 10 {
		values1[i] = rng1.Intn(1000)
		values2[i] = rng2.Intn(1000)
	}

	assert.NotEqual(t, values1, values2, "Different seeds should produce different sequences")
}

func TestSortIDsNumerically(t *testing.T) {
	ids := []string{"100", "5", "50", "1", "25"}
	sortIDsNumerically(ids)

	expected := []string{"1", "5", "25", "50", "100"}
	assert.Equal(t, expected, ids)
}

func TestSortIDsNumerically_AlreadySorted(t *testing.T) {
	ids := []string{"1", "2", "3", "4", "5"}
	sortIDsNumerically(ids)

	expected := []string{"1", "2", "3", "4", "5"}
	assert.Equal(t, expected, ids)
}

func TestSortIDsNumerically_SingleElement(t *testing.T) {
	ids := []string{"42"}
	sortIDsNumerically(ids)

	assert.Equal(t, []string{"42"}, ids)
}

func TestSortIDsNumerically_Empty(t *testing.T) {
	ids := []string{}
	sortIDsNumerically(ids)

	assert.Empty(t, ids)
}

// ── Edge Cases ────────────────────────────────────────────────────────────────

func TestShardByRoundRobin_SingleShard(t *testing.T) {
	ids := createTestIDs(10, 1)
	shards := shardByRoundRobin(ids, 1, "", nil)

	require.Len(t, shards, 1)
	assert.Len(t, shards[0], 10)
}

func TestShardByPercentage_SingleShard(t *testing.T) {
	ids := createTestIDs(10, 1)
	shards := shardByPercentage(ids, []int{100}, "", nil)

	require.Len(t, shards, 1)
	assert.Len(t, shards[0], 10)
}

func TestShardBySize_SingleShard(t *testing.T) {
	ids := createTestIDs(10, 1)
	shards := shardBySize(ids, []int{-1}, "", nil)

	require.Len(t, shards, 1)
	assert.Len(t, shards[0], 10)
}

func TestShardByRendezvous_SingleShard(t *testing.T) {
	ids := createTestIDs(10, 1)
	shards := shardByRendezvous(ids, 1, "test-seed", nil)

	require.Len(t, shards, 1)
	assert.Len(t, shards[0], 10)
}

func TestShardBySize_MultipleRemainderShards(t *testing.T) {
	ids := createTestIDs(100, 1)
	sizes := []int{10, -1}

	shards := shardBySize(ids, sizes, "", nil)

	require.Len(t, shards, 2)
	assert.Equal(t, 10, len(shards[0]))
	assert.Equal(t, 90, len(shards[1]))
}

func TestShardBySize_ZeroSizeWithReservations(t *testing.T) {
	ids := createTestIDs(10, 1)
	reservations := &shardReservations{
		IDsByShard: map[string][]string{
			"shard_0": {"1000", "1001", "1002", "1003", "1004"},
		},
		CountsByShard: map[int]int{
			0: 5,
		},
		UnreservedIDs: ids,
	}
	sizes := []int{5, 10}

	shards := shardBySize(ids, sizes, "", reservations)

	require.Len(t, shards, 2)
	assert.Equal(t, 5, len(shards[0]), "Shard 0 should have exactly 5 (all reserved, 0 distributed)")
	assert.Contains(t, shards[0], "1000")
}

// ── Test Utilities ────────────────────────────────────────────────────────────

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func findIDShard(id string, shards [][]string) int {
	for i, shard := range shards {
		for _, shardID := range shard {
			if shardID == id {
				return i
			}
		}
	}
	return -1
}
