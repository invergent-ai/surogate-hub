package api

import (
	"fmt"
	"net/http"
	"strings"

	gwerrors "github.com/invergent-ai/surogate-hub/pkg/gateway/errors"
	"github.com/invergent-ai/surogate-hub/pkg/gateway/operations"
	"github.com/invergent-ai/surogate-hub/pkg/gateway/sig"
)

func NewS3GatewayEndpointErrorHandler(gatewayDomains []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGatewayRequest(r) {
			handleGatewayRequest(w, r, gatewayDomains)
			return
		}

		// For other requests, return generic "not found" error
		w.WriteHeader(http.StatusNotFound)
	})
}

func handleGatewayRequest(w http.ResponseWriter, r *http.Request, gatewayDomains []string) {
	// s3 signed request reaching the ui handler, return an error response instead of the default path
	err := gwerrors.Codes[gwerrors.ERRHubWrongEndpoint]
	err.Description = fmt.Sprintf("%s (%v)", err.Description, gatewayDomains)
	o := operations.Operation{}
	o.EncodeError(w, r, nil, err)
}

func isGatewayRequest(r *http.Request) bool {
	// v4 and v2 header key are equal
	vals := r.Header.Values(sig.V4authHeaderName)
	for _, v := range vals {
		if strings.HasPrefix(v, sig.V4authHeaderPrefix) {
			return true
		}
		if sig.V2AuthHeaderRegexp.MatchString(v) {
			return true
		}
	}
	return false
}
