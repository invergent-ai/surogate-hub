package esti

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/api/apiutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestHubctlDoctor(t *testing.T) {
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	endPointURL := viper.GetString("endpoint_url") + apiutil.BaseURL
	u, err := url.Parse(endpointURL)
	require.NoError(t, err)
	vars := map[string]string{
		"LAKEFS_ENDPOINT": endPointURL,
		"HOST":            fmt.Sprintf("%s://%s", u.Scheme, u.Host),
	}

	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor", false, "hubctl_doctor_ok", vars)
	RunCmdAndVerifyFailureWithFile(t, hubctlLocation()+" doctor -c not_exits.yaml", false, "hubctl_doctor_not_exists_file", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor", false, "hubctl_doctor_wrong_endpoint", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor", false, "hubctl_doctor_wrong_uri_format_endpoint", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor", false, "hubctl_doctor_with_wrong_credentials", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor", false, "hubctl_doctor_with_suspicious_access_key_id", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor", false, "hubctl_doctor_with_wrong_credentials", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor", false, "hubctl_doctor_with_suspicious_secret_access_key", vars)

	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, secretAccessKey, endPointURL)+" doctor --verbose", false, "hubctl_doctor_ok_verbose", vars)
	RunCmdAndVerifyFailureWithFile(t, hubctlLocation()+" doctor -c not_exits.yaml --verbose", false, "hubctl_doctor_not_exists_file", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, secretAccessKey, endPointURL+"1")+" doctor --verbose", false, "hubctl_doctor_wrong_endpoint_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, secretAccessKey, "wrong_uri")+" doctor --verbose", false, "hubctl_doctor_wrong_uri_format_endpoint_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams("AKIAJZZZZZZZZZZZZZZQ", secretAccessKey, endPointURL)+" doctor --verbose", false, "hubctl_doctor_with_wrong_credentials_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams("AKIAJOI!COZ5JBYHCSDQ", secretAccessKey, endPointURL)+" doctor --verbose", false, "hubctl_doctor_with_suspicious_access_key_id_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", endPointURL)+" doctor --verbose", false, "hubctl_doctor_with_wrong_credentials_verbose", vars)
	RunCmdAndVerifySuccessWithFile(t, HubctlWithParams(accessKeyID, "TQG5JcovOozCGJnIRmIKH7Flq1tLxnuByi9/WmJ!", endPointURL)+" doctor --verbose", false, "hubctl_doctor_with_suspicious_secret_access_key_verbose", vars)
}
