package cmd

// normalization.go contains helpers that sanitise raw flag/config values
// before they are used by the rest of the command.  Each function trims
// whitespace or converts between types so that downstream code can assume
// clean inputs.

import (
	"fmt"
	"strconv"
	"strings"
)

// parseTrimmedIntSlice trims whitespace from each element of raw and converts
// to []int. Empty strings are skipped so that an unset flag returns nil.
func parseTrimmedIntSlice(raw []string) ([]int, error) {
	out := make([]int, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", s, err)
		}
		out = append(out, n)
	}
	return out, nil
}
