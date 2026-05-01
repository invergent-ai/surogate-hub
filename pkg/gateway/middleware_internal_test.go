package gateway

import (
	"fmt"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/graveler"
)

func TestIsRepositoryMissingError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "not found",
			err:  graveler.ErrNotFound,
			want: true,
		},
		{
			name: "invalid repository id",
			err:  fmt.Errorf("argument repository: %w", graveler.ErrInvalidRepositoryID),
			want: true,
		},
		{
			name: "other error",
			err:  graveler.ErrPreconditionFailed,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRepositoryMissingError(tt.err)
			if got != tt.want {
				t.Fatalf("isRepositoryMissingError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
