package uri_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/uri"
)

func strp(v string) *string {
	return &v
}

func TestParse(t *testing.T) {
	cases := []struct {
		Input    string
		Err      error
		Expected *uri.URI
	}{
		{
			Input: "sg://alice/model/main/data",
			Expected: &uri.URI{
				Repository: "alice/model",
				Ref:        "main",
				Path:       strp("data"),
			},
		},
		{
			Input: "sg://alice/model",
			Expected: &uri.URI{
				Repository: "alice/model",
			},
		},
		{
			Input: "sg://alice/model/main/baz/path",
			Expected: &uri.URI{
				Repository: "alice/model",
				Ref:        "main",
				Path:       strp("baz/path"),
			},
		},
		{
			Input: "sg://alice/model/main/baz/path@withappendix.foo",
			Expected: &uri.URI{
				Repository: "alice/model",
				Ref:        "main",
				Path:       strp("baz/path@withappendix.foo"),
			},
		},
		{
			Input: "sg://alice-dev/fo-o/main/baz/path@withappendix.foo",
			Expected: &uri.URI{
				Repository: "alice-dev/fo-o",
				Ref:        "main",
				Path:       strp("baz/path@withappendix.foo"),
			},
		},
		{
			Input: "sg://foo",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "sg://alice/model/main/",
			Expected: &uri.URI{
				Repository: "alice/model",
				Ref:        "main",
				Path:       strp(""),
			},
		},
		{
			Input: "sg://alice/model/main//",
			Expected: &uri.URI{
				Repository: "alice/model",
				Ref:        "main",
				Path:       strp("/"),
			},
		},
		{
			Input: "sg://foo/bar",
			Expected: &uri.URI{
				Repository: "foo/bar",
			},
		},
		{
			Input: "sg://foo@bar",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "sg://foo@bar/baz",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "sggg://foo/bar/baz",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "sg:/foo/bar/baz",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "sg//foo/bar/baz",
			Err:   uri.ErrMalformedURI,
		},
	}

	for i, test := range cases {
		u, err := uri.Parse(test.Input)
		if test.Err != nil {
			if !errors.Is(err, test.Err) {
				t.Fatalf("case (%d) - expected error %v for input %s, got error: %v", i, test.Err, test.Input, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("case (%d) - expected no error for input %s, got error: %v", i, test.Input, err)
		}
		if !uri.Equals(u, test.Expected) {
			t.Fatalf("case (%d) - expected uri '%s' for input '%s', got uri: '%v'", i, test.Expected, test.Input, u)
		}
	}
}

func TestURI_String(t *testing.T) {
	cases := []struct {
		Input    *uri.URI
		Expected string
	}{
		{&uri.URI{
			Repository: "alice/model",
			Ref:        "main",
			Path:       strp("baz/file.csv"),
		}, "sg://alice/model/main/baz/file.csv"},
		{&uri.URI{
			Repository: "alice/model",
			Ref:        "main",
			Path:       strp(""),
		}, "sg://alice/model/main/"},
		{&uri.URI{
			Repository: "alice/model",
			Ref:        "main",
		}, "sg://alice/model/main"},
		{&uri.URI{
			Repository: "alice/model",
		}, "sg://alice/model"},
	}

	for i, test := range cases {
		if !strings.EqualFold(test.Input.String(), test.Expected) {
			t.Fatalf("case (%d) - expected '%s', got '%s'", i, test.Expected, test.Input.String())
		}
	}
}

func TestIsValid(t *testing.T) {
	cases := []struct {
		Input    string
		Expected bool
	}{
		{"sg://alice/model/main", true},
	}

	for i, test := range cases {
		if uri.IsValid(test.Input) != test.Expected {
			t.Fatalf("case (%d) - expected %v, got %v", i, test.Expected, uri.IsValid(test.Input))
		}
	}
}

func TestMust(t *testing.T) {
	// should not panic
	u := uri.Must(uri.Parse("sg://alice/model/main"))
	if !uri.Equals(u, &uri.URI{
		Repository: "alice/model",
		Ref:        "main",
	}) {
		t.Fatalf("expected a parsed URI according to input, instead got %s", u.String())
	}
	recovered := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
			}
		}()
		uri.Must(uri.Parse("sggggggg://foo/bar"))
	}()

	if !recovered {
		t.Fatalf("expected parsing to cause a panic, it didnt")
	}
}
