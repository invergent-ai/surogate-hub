package graveler

import "testing"

func TestRepoPathEscapesRepositoryID(t *testing.T) {
	got := RepoPath("alice/model")
	const expected = "repos/alice%2Fmodel"
	if got != expected {
		t.Fatalf("RepoPath() = %q, expected %q", got, expected)
	}
}
