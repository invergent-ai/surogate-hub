package esti

import "testing"

func TestNormalizeRandomObjectKeyLocalAdapterPath(t *testing.T) {
	const (
		storage = "local://system-testing/run/repo"
		output  = "Physical Address: local:///tmp/sghub/data/system-testing/run/repo/data/abcdeabcdeabcdeabcde/fghijfghijfghijfghij\n"
		want    = "Physical Address: local://system-testing/run/repo/<OBJECT_KEY>\n"
	)

	if got := normalizeRandomObjectKey(output, storage); got != want {
		t.Fatalf("normalizeRandomObjectKey() = %q, want %q", got, want)
	}
}
