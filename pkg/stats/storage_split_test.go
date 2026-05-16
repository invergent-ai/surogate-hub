package stats_test

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/stats"
)

func TestSplitNamespacedRepo(t *testing.T) {
	cases := []struct {
		in          string
		wantOwner   string
		wantName    string
		wantErrText string
	}{
		{"alice/training", "alice", "training", ""},
		{"alice/training/sub", "alice", "training/sub", ""},
		{"", "", "", "not an owner/name id"},
		{"/training", "", "", "not an owner/name id"},
		{"alice/", "", "", "not an owner/name id"},
		{"alice", "", "", "not an owner/name id"},
	}
	for _, tc := range cases {
		owner, name, err := stats.SplitNamespacedRepo(tc.in)
		if tc.wantErrText == "" {
			if err != nil {
				t.Errorf("%q: unexpected err %v", tc.in, err)
				continue
			}
			if owner != tc.wantOwner || name != tc.wantName {
				t.Errorf("%q = (%q,%q), want (%q,%q)", tc.in, owner, name, tc.wantOwner, tc.wantName)
			}
		} else if err == nil {
			t.Errorf("%q: expected error %q, got nil", tc.in, tc.wantErrText)
		}
	}
}
