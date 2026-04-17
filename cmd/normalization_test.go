package cmd

// normalization_test.go contains unit tests for every normalisation helper in
// normalization.go.
//
//   TestParseTrimmedIntSlice — whitespace trimming and int conversion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTrimmedIntSlice(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    []int
		wantErr bool
	}{
		{
			name:  "clean input no spaces",
			input: []string{"25", "25", "50"},
			want:  []int{25, 25, 50},
		},
		{
			name:  "space-padded values (user types '25, 25, 50')",
			input: []string{"25", " 25", " 50"},
			want:  []int{25, 25, 50},
		},
		{
			name:  "tabs and mixed whitespace",
			input: []string{" 10 ", "\t30\t", " 60"},
			want:  []int{10, 30, 60},
		},
		{
			name:  "empty strings are skipped",
			input: []string{""},
			want:  []int{},
		},
		{
			name:  "nil input returns empty",
			input: nil,
			want:  []int{},
		},
		{
			name:    "letters only returns error",
			input:   []string{"abc"},
			wantErr: true,
		},
		{
			name:    "letters mixed with digits returns error",
			input:   []string{"25", "abc", "50"},
			wantErr: true,
		},
		{
			name:    "special characters returns error",
			input:   []string{"25!"},
			wantErr: true,
		},
		{
			name:    "float value returns error",
			input:   []string{"2.5"},
			wantErr: true,
		},
		{
			name:    "internal space returns error",
			input:   []string{"2 5"},
			wantErr: true,
		},
		{
			name:    "scientific notation returns error",
			input:   []string{"1e3"},
			wantErr: true,
		},
		{
			name:    "integer overflow returns error",
			input:   []string{"99999999999999999999"},
			wantErr: true,
		},
		{
			name:  "negative values preserved",
			input: []string{"50", "200", "-1"},
			want:  []int{50, 200, -1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTrimmedIntSlice(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
