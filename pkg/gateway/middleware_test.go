package gateway_test

import (
	"reflect"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/gateway"
)

func TestParseRequestParts(t *testing.T) {
	bareDomains := []string{"sghub.example.com"}
	cases := []struct {
		Name           string
		URLPath        string
		Host           string
		ResultSuccess  bool
		ExpectedResult gateway.RequestParts
	}{
		{
			Name:    "repo_only_virtual_style",
			URLPath: "/",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "",
				Path:        "",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_only_virtual_style_1",
			URLPath: "",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "",
				Path:        "",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_only_path_style",
			URLPath: "/foo",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "",
				Path:        "",
				MatchedHost: false,
			},
		},
		{
			Name:    "repo_only_path_style_1",
			URLPath: "/foo/",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "",
				Path:        "",
				MatchedHost: false,
			},
		},
		{
			Name:    "repo_only_path_style_2",
			URLPath: "foo/",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "",
				Path:        "",
				MatchedHost: false,
			},
		},
		{
			Name:    "repo_branch_virtual_style",
			URLPath: "/bar",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_virtual_style_1",
			URLPath: "/bar/",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_virtual_style_2",
			URLPath: "bar/",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_virtual_style_3",
			URLPath: "bar",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_path_virtual_style",
			URLPath: "bar/a/b/c",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_path_virtual_style_1",
			URLPath: "/bar/a/b/c",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_path_virtual_style_2",
			URLPath: "bar/a/b/c/",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c/",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_path_virtual_style_3",
			URLPath: "/bar/a/b/c/",
			Host:    "foo.sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c/",
				MatchedHost: true,
			},
		},
		{
			Name:    "repo_branch_path_path_style",
			URLPath: "foo/bar/a/b/c",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c",
				MatchedHost: false,
			},
		},
		{
			Name:    "repo_branch_path_path_style_1",
			URLPath: "/foo/bar/a/b/c",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c",
				MatchedHost: false,
			},
		},
		{
			Name:    "repo_branch_path_path_style_2",
			URLPath: "foo/bar/a/b/c/",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c/",
				MatchedHost: false,
			},
		},
		{
			Name:    "repo_branch_path_path_style_3",
			URLPath: "/foo/bar/a/b/c/",
			Host:    "sghub.dev",
			ExpectedResult: gateway.RequestParts{
				Repository:  "foo",
				Ref:         "bar",
				Path:        "a/b/c/",
				MatchedHost: false,
			},
		},
		{
			Name:    "all_empty",
			URLPath: "",
			Host:    "sghub.example.com",
			ExpectedResult: gateway.RequestParts{
				Repository:  "",
				Ref:         "",
				Path:        "",
				MatchedHost: true,
			},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			got := gateway.ParseRequestParts(cas.Host, cas.URLPath, bareDomains)
			if !reflect.DeepEqual(cas.ExpectedResult, got) {
				t.Errorf("expected parts = %+v for split '%s', got %+v", cas.ExpectedResult, cas.URLPath, got)
			}
		})
	}
}
