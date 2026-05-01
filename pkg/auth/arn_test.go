package auth_test

import (
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/auth"
)

func TestParseARN(t *testing.T) {
	cases := []struct {
		Input string
		Arn   auth.Arn
		Error bool
	}{
		{Input: "", Error: true},
		{Input: "arn:sghub:repo", Error: true},
		{Input: "arn:sghub:repos:a:b:myrepo", Arn: auth.Arn{
			Partition:  "sghub",
			Service:    "repos",
			Region:     "a",
			AccountID:  "b",
			ResourceID: "myrepo"}},
		{Input: "arn:sghub:repos:a::myrepo", Arn: auth.Arn{
			Partition:  "sghub",
			Service:    "repos",
			Region:     "a",
			AccountID:  "",
			ResourceID: "myrepo"}},
		{Input: "arn:sghub:repos::b:myrepo", Arn: auth.Arn{
			Partition:  "sghub",
			Service:    "repos",
			Region:     "",
			AccountID:  "b",
			ResourceID: "myrepo"}},
		{Input: "arn:sghub:repos:::myrepo", Arn: auth.Arn{
			Partition:  "sghub",
			Service:    "repos",
			Region:     "",
			AccountID:  "",
			ResourceID: "myrepo"}},
		{Input: "arn:sghub:fs:::myrepo/branch/file:with:colon", Arn: auth.Arn{
			Partition:  "sghub",
			Service:    "fs",
			Region:     "",
			AccountID:  "",
			ResourceID: "myrepo/branch/file:with:colon"}},
	}

	for _, c := range cases {
		got, err := auth.ParseARN(c.Input)
		if err != nil && !c.Error {
			t.Fatalf("got unexpected error parsing arn: \"%s\": \"%s\"", c.Input, err)
		} else if err != nil {
			continue
		} else if c.Error {
			t.Fatalf("expected error parsing arn: \"%s\"", c.Input)
		}
		if got.AccountID != c.Arn.AccountID {
			t.Fatalf("got unexpected account ID parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.AccountID, c.Arn.AccountID)
		}
		if got.Region != c.Arn.Region {
			t.Fatalf("got unexpected region parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.Region, c.Arn.Region)
		}
		if got.Partition != c.Arn.Partition {
			t.Fatalf("got unexpected partition parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.Partition, c.Arn.Partition)
		}
		if got.Service != c.Arn.Service {
			t.Fatalf("got unexpected service parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.Service, c.Arn.Service)
		}
		if got.ResourceID != c.Arn.ResourceID {
			t.Fatalf("got unexpected resource ID parsing arn: \"%s\": \"%s\" (expected \"%s\")", c.Input, got.ResourceID, c.Arn.ResourceID)
		}
	}
}

func TestArnMatch(t *testing.T) {
	cases := []struct {
		InputSource      string
		InputDestination string
		Match            bool
	}{
		{"arn:sghub:repos::b:myrepo", "arn:sghub:repos::b:myrepo", true},
		{"arn:sghub:repos::b:*", "arn:sghub:repos::b:myrepo", true},
		{"arn:sghub:repos::b:my*", "arn:sghub:repos::b:myrepo", true},
		{"arn:sghub:repos::b:my*po", "arn:sghub:repos::b:myrepo", true},
		{"arn:sghub:repos::b:our*", "arn:sghub:repos::b:myrepo", false},
		{"arn:sghub:repos::b:my*own", "arn:sghub:repos::b:myrepo", false},
		{"arn:sghub:repos::b:myrepo", "arn:sghub:repos::b:*", false},
		{"arn:sghub:repo:::*", "arn:sghub:repo:::*", true},
		{"arn:sghub:repo", "arn:sghub:repo", false},
	}

	for _, c := range cases {
		got := auth.ArnMatch(c.InputSource, c.InputDestination)
		if got != c.Match {
			t.Fatalf("expected match %v, got %v on source = %s, destination = %s", c.Match, got, c.InputSource, c.InputDestination)
		}
	}
}
