package cmd

// strategies.go contains the four sharding algorithms, adapted from the
// terraform-provider-jamfpro guid_list_sharder data source. All Terraform and
// tflog dependencies have been removed; the logic is otherwise identical.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
)

// shardByRoundRobin distributes IDs in circular order, guaranteeing equal
// shard sizes ±1. If a seed is provided, IDs are sorted numerically then
// shuffled deterministically before distribution.
//
// Algorithm: Round-robin scheduling
// Reference: https://en.wikipedia.org/wiki/Round-robin_scheduling
func shardByRoundRobin(ids []string, shardCount int, seed string, reservations *shardReservations) [][]string {
	if shardCount <= 0 {
		shardCount = 1
	}

	unreservedIDs := ids
	if reservations != nil {
		unreservedIDs = reservations.UnreservedIDs
	}

	shards := make([][]string, shardCount)
	distributionIDs := sortAndShuffleIfSeed(unreservedIDs, seed)

	for i, id := range distributionIDs {
		shards[i%shardCount] = append(shards[i%shardCount], id)
	}

	if reservations != nil {
		for shardName, reservedIDs := range reservations.IDsByShard {
			var idx int
			fmt.Sscanf(shardName, "shard_%d", &idx)
			shards[idx] = append(reservedIDs, shards[idx]...)
		}
	}

	for i := range shards {
		sortIDsNumerically(shards[i])
	}

	return shards
}

// shardByPercentage distributes IDs according to specified percentages.
// Target shard sizes are calculated against total ID count (after exclusions).
// Reserved counts are subtracted from targets to maintain percentage accuracy.
// The last shard receives any remainder from rounding.
func shardByPercentage(ids []string, percentages []int, seed string, reservations *shardReservations) [][]string {
	unreservedIDs := ids
	totalIDs := len(ids)

	if reservations != nil {
		unreservedIDs = reservations.UnreservedIDs
	}

	shardCount := len(percentages)
	shards := make([][]string, shardCount)

	if len(unreservedIDs) == 0 {
		return shards
	}

	distributionIDs := sortAndShuffleIfSeed(unreservedIDs, seed)

	currentIndex := 0
	for i, percentage := range percentages {
		var shardSize int
		if i == shardCount-1 {
			shardSize = len(unreservedIDs) - currentIndex
		} else {
			shardSize = int(float64(totalIDs) * float64(percentage) / 100.0)
			if reservations != nil {
				shardSize -= reservations.CountsByShard[i]
			}
		}

		if currentIndex+shardSize > len(unreservedIDs) {
			shardSize = len(unreservedIDs) - currentIndex
		}

		if shardSize > 0 {
			shards[i] = distributionIDs[currentIndex : currentIndex+shardSize]
			currentIndex += shardSize
		}
	}

	if reservations != nil {
		for shardName, reservedIDs := range reservations.IDsByShard {
			var idx int
			fmt.Sscanf(shardName, "shard_%d", &idx)
			shards[idx] = append(reservedIDs, shards[idx]...)
		}
	}

	for i := range shards {
		sortIDsNumerically(shards[i])
	}

	return shards
}

// shardBySize distributes IDs according to specified absolute sizes.
// A value of -1 in the last position means "all remaining IDs".
// Reserved counts are subtracted from targets so the final shard size
// (distributed + reserved) matches the requested size.
func shardBySize(ids []string, sizes []int, seed string, reservations *shardReservations) [][]string {
	unreservedIDs := ids
	if reservations != nil {
		unreservedIDs = reservations.UnreservedIDs
	}

	shardCount := len(sizes)
	shards := make([][]string, shardCount)

	if len(unreservedIDs) == 0 {
		return shards
	}

	distributionIDs := sortAndShuffleIfSeed(unreservedIDs, seed)

	currentIndex := 0
	for i, size := range sizes {
		var shardSize int

		if size == -1 {
			shardSize = len(unreservedIDs) - currentIndex
		} else {
			shardSize = size
			if reservations != nil {
				shardSize -= reservations.CountsByShard[i]
			}
			if currentIndex+shardSize > len(unreservedIDs) {
				shardSize = len(unreservedIDs) - currentIndex
			}
		}

		if shardSize > 0 && currentIndex < len(unreservedIDs) {
			shards[i] = distributionIDs[currentIndex : currentIndex+shardSize]
			currentIndex += shardSize
		} else {
			shards[i] = []string{}
		}
	}

	if reservations != nil {
		for shardName, reservedIDs := range reservations.IDsByShard {
			var idx int
			fmt.Sscanf(shardName, "shard_%d", &idx)
			shards[idx] = append(reservedIDs, shards[idx]...)
		}
	}

	for i := range shards {
		sortIDsNumerically(shards[i])
	}

	return shards
}

// shardByRendezvous distributes IDs using Highest Random Weight (HRW) algorithm.
// Always deterministic. Provides superior stability when shard count changes —
// only ~1/n IDs move when a new shard is added.
//
// Algorithm: Rendezvous Hashing (Highest Random Weight Hashing)
// Reference: https://en.wikipedia.org/wiki/Rendezvous_hashing
// Original Paper: Thaler & Ravishankar (1998)
func shardByRendezvous(ids []string, shardCount int, seed string, reservations *shardReservations) [][]string {
	if shardCount <= 0 {
		shardCount = 1
	}

	unreservedIDs := ids
	if reservations != nil {
		unreservedIDs = reservations.UnreservedIDs
	}

	shards := make([][]string, shardCount)
	for i := range shardCount {
		shards[i] = []string{}
	}

	for _, id := range unreservedIDs {
		highestWeight := uint64(0)
		selectedShard := 0

		for shardIdx := range shardCount {
			input := fmt.Sprintf("%s:shard_%d:%s", id, shardIdx, seed)
			hash := sha256.Sum256([]byte(input))
			weight := binary.BigEndian.Uint64(hash[:8])

			if weight > highestWeight {
				highestWeight = weight
				selectedShard = shardIdx
			}
		}

		shards[selectedShard] = append(shards[selectedShard], id)
	}

	if reservations != nil {
		for shardName, reservedIDs := range reservations.IDsByShard {
			var idx int
			fmt.Sscanf(shardName, "shard_%d", &idx)
			shards[idx] = append(reservedIDs, shards[idx]...)
		}
	}

	for i := range shards {
		sortIDsNumerically(shards[i])
	}

	return shards
}

// sortAndShuffleIfSeed sorts IDs numerically, then shuffles deterministically
// using the seed. Returns IDs unchanged (in API order) when seed is empty.
func sortAndShuffleIfSeed(ids []string, seed string) []string {
	if seed == "" {
		return ids
	}

	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sortIDsNumerically(sorted)
	return shuffleIDs(sorted, seed)
}

// shuffleIDs performs a deterministic Fisher-Yates shuffle seeded from the
// given string. Returns a new slice; the original is not mutated.
//
// Algorithm: Fisher-Yates shuffle (Knuth shuffle)
// Reference: https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
func shuffleIDs(ids []string, seed string) []string {
	rng := createSeededRNG(seed)
	shuffled := make([]string, len(ids))
	copy(shuffled, ids)

	for i := len(shuffled) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled
}

// createSeededRNG derives a deterministic *rand.Rand from a seed string by
// hashing it with SHA-256 and reading the first 8 bytes as a uint64.
func createSeededRNG(seed string) *rand.Rand {
	hash := sha256.Sum256([]byte(seed))
	seedValue := int64(binary.BigEndian.Uint64(hash[:8]))
	return rand.New(rand.NewSource(seedValue))
}

// sortIDsNumerically sorts a string-ID slice by numeric value in-place.
func sortIDsNumerically(ids []string) {
	slices.SortFunc(ids, func(a, b string) int {
		aInt, _ := strconv.Atoi(a)
		bInt, _ := strconv.Atoi(b)
		return aInt - bInt
	})
}
