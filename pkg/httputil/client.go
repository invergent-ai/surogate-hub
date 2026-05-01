package httputil

import "net/http"

// GetRequestHubClient get Surogate Hub client identifier from request.
//
//	It extracts the data from X-SgHub-Client header and fallback to the user-agent
func GetRequestHubClient(r *http.Request) string {
	id := r.Header.Get("X-SgHub-Client")
	if id == "" {
		id = r.UserAgent()
	}
	return id
}
