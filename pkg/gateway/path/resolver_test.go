package path

import (
	"reflect"
	"strings"
	"testing"
)

func TestResolvePath(t *testing.T) {
	type args struct {
		encodedPath string
	}
	tests := []struct {
		name    string
		args    args
		want    ResolvedPath
		wantErr bool
	}{
		{
			name: "branch with root",
			args: args{encodedPath: "/Branch-1"},
			want: ResolvedPath{Ref: "Branch-1", Path: "", WithPath: false},
		},
		{
			name: "branch without root",
			args: args{encodedPath: "Branch-1"},
			want: ResolvedPath{Ref: "Branch-1", Path: "", WithPath: false},
		},
		{
			name: "branch and path",
			args: args{encodedPath: "Branch-1/dir1/file1"},
			want: ResolvedPath{Ref: "Branch-1", Path: "dir1/file1", WithPath: true},
		},
		{
			name: "branch ends with delimiter",
			args: args{encodedPath: "Branch-1/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "", WithPath: true},
		},
		{
			name: "branch with path ends with delimiter",
			args: args{encodedPath: "Branch-1/path2/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "path2/", WithPath: true},
		},
		{
			name: "branch with path both start and end with delimiter",
			args: args{encodedPath: "/Branch-1/path2/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "path2/", WithPath: true},
		},
		{
			name: "branch with path that start and end with delimiter",
			args: args{encodedPath: "Branch-1//path3/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "/path3/", WithPath: true},
		},
		{
			name: "empty",
			args: args{},
			want: ResolvedPath{},
		},
		{
			name: "have some space",
			args: args{encodedPath: "BrAnCh99/ a dir / a file"},
			want: ResolvedPath{
				Ref:      "BrAnCh99",
				Path:     " a dir / a file",
				WithPath: true,
			},
		},
		{
			name: "ref with tilde",
			args: args{encodedPath: "main~1"},
			want: ResolvedPath{Ref: "main~1"},
		},
		{
			name: "ref with tilde and multiple separators",
			args: args{encodedPath: "main~1//a/b"},
			want: ResolvedPath{Ref: "main~1", Path: "/a/b", WithPath: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolvePath(tt.args.encodedPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("ResolvePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ResolvePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveAbsolutePath(t *testing.T) {
	namespacedBucket := RepositoryIDToBucket("alice/model")
	tests := []struct {
		name        string
		encodedPath string
		want        ResolvedAbsolutePath
		wantErr     bool
	}{
		{
			name:        "namespaced repo branch and path",
			encodedPath: "/alice/model/main/data/file.txt",
			want: ResolvedAbsolutePath{
				Repo:      "alice/model",
				Reference: "main",
				Path:      "data/file.txt",
			},
		},
		{
			name:        "namespaced repo branch and empty path",
			encodedPath: "alice/model/main/",
			want: ResolvedAbsolutePath{
				Repo:      "alice/model",
				Reference: "main",
				Path:      "",
			},
		},
		{
			name:        "encoded bucket repo branch and path",
			encodedPath: "/" + namespacedBucket + "/main/data/file.txt",
			want: ResolvedAbsolutePath{
				Repo:      "alice/model",
				Reference: "main",
				Path:      "data/file.txt",
			},
		},
		{
			name:        "single segment repo is malformed",
			encodedPath: "repo/main/data",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveAbsolutePath(tt.encodedPath)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ResolveAbsolutePath() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ResolveAbsolutePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepositoryIDBucketRoundTrip(t *testing.T) {
	repository := "test-user/tests3copyobjectmultipart"
	bucket := RepositoryIDToBucket(repository)
	if strings.Contains(bucket, "/") {
		t.Fatalf("RepositoryIDToBucket(%q) = %q contains slash", repository, bucket)
	}
	got, ok := BucketToRepositoryID(bucket)
	if !ok {
		t.Fatalf("BucketToRepositoryID(%q) failed", bucket)
	}
	if got != repository {
		t.Fatalf("BucketToRepositoryID(%q) = %q, want %q", bucket, got, repository)
	}
}

func TestBucketToRepositoryIDIgnoresOrdinaryBucketNames(t *testing.T) {
	for _, bucket := range []string{"s3", "staging-data", "sample-bucket"} {
		if got, ok := BucketToRepositoryID(bucket); ok {
			t.Fatalf("BucketToRepositoryID(%q) = %q, want no match", bucket, got)
		}
	}
}
