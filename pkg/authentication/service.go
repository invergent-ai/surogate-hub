package authentication

//go:generate go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.5.6 -package apiclient -generate "types,client" -o apiclient/client.gen.go  ../../api/authentication.yml
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -package=mock -destination=mock/mock_authentication_client.go github.com/treeverse/lakefs/pkg/authentication/apiclient ClientWithResponsesInterface

import (
	"context"

	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
)

type Service interface {
	IsExternalPrincipalsEnabled() bool
	ExternalPrincipalLogin(ctx context.Context, identityRequest map[string]interface{}) (*apiclient.ExternalPrincipal, error)
	// ValidateSTS validates the STS parameters and returns the external user ID
	ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error)
}

type DummyService struct{}

func NewDummyService() *DummyService {
	return &DummyService{}
}

func (d DummyService) ValidateSTS(_ context.Context, _, _, _ string) (string, error) {
	return "", ErrNotImplemented
}

func (d DummyService) ExternalPrincipalLogin(_ context.Context, _ map[string]interface{}) (*apiclient.ExternalPrincipal, error) {
	return nil, ErrNotImplemented
}

func (d DummyService) IsExternalPrincipalsEnabled() bool {
	return false
}
