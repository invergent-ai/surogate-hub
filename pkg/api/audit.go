package api

import "github.com/invergent-ai/surogate-hub/pkg/version"

type AuditChecker interface {
	LastCheck() (*version.AuditResponse, error)
	CheckLatestVersion() (*version.LatestVersionResponse, error)
}
