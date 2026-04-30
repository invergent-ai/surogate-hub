package api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testHTTPGetPage(t *testing.T, handler http.Handler, url string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("Failed fetching url '%s': %s", url, err)
	}
	handler.ServeHTTP(rr, req)
	return rr
}

func TestNewS3GatewayEndpointErrorHandler_GatewayError(t *testing.T) {
	handler := NewS3GatewayEndpointErrorHandler([]string{"s3.lakefs.dev"})
	rr := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 1234567890")
	handler.ServeHTTP(rr, req)

	// verify aws xml error not found
	expectedStatusCode := http.StatusNotFound
	if rr.Code != expectedStatusCode {
		t.Fatalf("Request status=%d, expected=%d", rr.Code, expectedStatusCode)
	}

	var errMsg struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
	}
	err = xml.Unmarshal(rr.Body.Bytes(), &errMsg)
	if err != nil {
		t.Fatal("Message unmarshal failed:", err)
	}
	const expectedErrorCode = "ERRLakeFSWrongEndpoint"
	if errMsg.Code != expectedErrorCode {
		t.Fatalf("Invalid XML Code '%s', expected '%s' - response body '%s'",
			errMsg.Code, expectedErrorCode, rr.Body.String())
	}
}
