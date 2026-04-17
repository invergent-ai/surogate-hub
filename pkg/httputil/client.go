package httputil

import "net/http"

// GetRequestLakeFSClient get Surogate Hub client identifier from request.
//
//	It extracts the data from X-Lakefs-Client header and fallback to the user-agent
func GetRequestLakeFSClient(r *http.Request) string {
	id := r.Header.Get("X-Lakefs-Client")
	if id == "" {
		id = r.UserAgent()
	}
	return id
}
