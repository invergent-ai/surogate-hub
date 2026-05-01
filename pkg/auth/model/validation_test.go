package model

import "testing"

func TestValidateArnNamespacedRepositoryResources(t *testing.T) {
	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{
			name: "repository",
			arn:  "arn:sghub:fs:::repository/alice/model",
		},
		{
			name: "object",
			arn:  "arn:sghub:fs:::repository/alice/model/object/data/file.txt",
		},
		{
			name: "branch",
			arn:  "arn:sghub:fs:::repository/alice/model/branch/main",
		},
		{
			name: "tag",
			arn:  "arn:sghub:fs:::repository/alice/model/tag/v1",
		},
		{
			name: "wildcard repository in owner namespace",
			arn:  "arn:sghub:fs:::repository/alice/*",
		},
		{
			name: "wildcard objects in repository",
			arn:  "arn:sghub:fs:::repository/alice/model/object/*",
		},
		{
			name:    "legacy object arn without owner",
			arn:     "arn:sghub:fs:::repository/foo/object/*",
			wantErr: true,
		},
		{
			name:    "legacy branch arn without owner",
			arn:     "arn:sghub:fs:::repository/foo/branch/main",
			wantErr: true,
		},
		{
			name:    "legacy repository arn without owner",
			arn:     "arn:sghub:fs:::repository/foo",
			wantErr: true,
		},
		{
			name:    "unknown repository subresource",
			arn:     "arn:sghub:fs:::repository/alice/model/policy/*",
			wantErr: true,
		},
		{
			name: "non repository fs resource",
			arn:  "arn:sghub:fs:::namespace/storage://bucket/path",
		},
		{
			name: "all resources",
			arn:  "*",
		},
		{
			name: "auth resource",
			arn:  "arn:sghub:auth:::user/alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateArn(tt.arn)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidateArn(%q) succeeded, expected error", tt.arn)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateArn(%q) returned unexpected error: %v", tt.arn, err)
			}
		})
	}
}
