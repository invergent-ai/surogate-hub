package cmd

import (
	"testing"
)

func TestMustParseNamespacedURIs(t *testing.T) {
	oldBaseURI := baseURI
	baseURI = ""
	t.Cleanup(func() { baseURI = oldBaseURI })

	repo := MustParseRepoURI("repository URI", "sg://alice/model")
	if repo.Repository != "alice/model" || repo.Ref != "" || repo.Path != nil {
		t.Fatalf("repo URI parsed incorrectly: %+v", repo)
	}

	branch := MustParseBranchURI("branch URI", "sg://alice/model/main")
	if branch.Repository != "alice/model" || branch.Ref != "main" || branch.Path != nil {
		t.Fatalf("branch URI parsed incorrectly: %+v", branch)
	}

	ref := MustParseRefURI("ref URI", "sg://alice/model/abc123")
	if ref.Repository != "alice/model" || ref.Ref != "abc123" || ref.Path != nil {
		t.Fatalf("ref URI parsed incorrectly: %+v", ref)
	}

	path := MustParsePathURI("path URI", "sg://alice/model/main/data/file.txt")
	if path.Repository != "alice/model" || path.Ref != "main" || path.Path == nil || *path.Path != "data/file.txt" {
		t.Fatalf("path URI parsed incorrectly: %+v", path)
	}
}

func TestIsValidAccessKeyID(t *testing.T) {
	type args struct {
		accessKeyID string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid access key id", args: args{accessKeyID: "AKIAJ12ZZZZZZZZZZZZQ"}, want: true},
		{name: "access key id with lower case char", args: args{accessKeyID: "AKIAJ12zZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with invalid char", args: args{accessKeyID: "AKIAJ12!ZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with extra char", args: args{accessKeyID: "AKIAJ123ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with missing char", args: args{accessKeyID: "AKIAJ1ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with wrong prefix", args: args{accessKeyID: "AKIAM12ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with wrong suffix", args: args{accessKeyID: "AKIAJ12ZZZZZZZZZZZZA"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidAccessKeyID(tt.args.accessKeyID); got != tt.want {
				t.Errorf("IsValidAccessKeyID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsValidSecretAccessKey(t *testing.T) {
	type args struct {
		secretAccessKey string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid secret access key", args: args{secretAccessKey: "TQG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: true},
		{name: "secret access key id with invalid char", args: args{secretAccessKey: "!QG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
		{name: "secret access key id with extra char", args: args{secretAccessKey: "aTQG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
		{name: "secret access key id with missing char", args: args{secretAccessKey: "QG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidSecretAccessKey(tt.args.secretAccessKey); got != tt.want {
				t.Errorf("IsValidSecretAccessKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
