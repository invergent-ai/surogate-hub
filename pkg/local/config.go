package local

import "github.com/invergent-ai/surogate-hub/pkg/api/apiutil"

const (
	// DefaultDirectoryPermissions Octal representation of default folder permissions
	DefaultDirectoryPermissions = 0o040777
	ClientMtimeMetadataKey      = apiutil.HubMetadataPrefix + "client-mtime"
)

type SyncFlags struct {
	Parallelism      int
	Presign          bool
	PresignMultipart bool
	NoProgress       bool
}

type Config struct {
	SyncFlags
	// SkipNonRegularFiles - By default hubctl local fails if local directory contains irregular files. When set, hubctl will skip these files instead.
	SkipNonRegularFiles bool
	// IncludePerm - Experimental: preserve Unix file permissions
	IncludePerm bool
	IncludeUID  bool
	IncludeGID  bool
}
