package esti

import "testing"

func TestSgHubHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, SgHub(), false, "sghub/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, SgHub()+" --help", false, "sghub/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, SgHub(), true, "sghub/help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, SgHub()+" --help", true, "sghub/help", emptyVars)
}

func TestHubSuperuser_basic(t *testing.T) {
	RequirePostgresDB(t)
	hubCmd := SgHub()
	outputString := "credentials:\n  access_key_id: <ACCESS_KEY_ID>\n  secret_access_key: <SECRET_ACCESS_KEY>\n"
	username := t.Name()
	expectFailure := false
	if isBasicAuth() {
		hubCmd = HubWithBasicAuth()
		outputString = "already exists"
		expectFailure = true
	}
	runCmdAndVerifyContainsText(t, hubCmd+" superuser --user-name "+username, expectFailure, false, outputString, nil)
}

func TestHubSuperuser_alreadyExists(t *testing.T) {
	RequirePostgresDB(t)
	hubCmd := SgHub()
	if isBasicAuth() {
		hubCmd = HubWithBasicAuth()
	}
	// On init - the AdminUsername is already created and expected error should be "already exist" (also in basic auth mode)
	RunCmdAndVerifyFailureContainsText(t, hubCmd+" superuser --user-name "+AdminUsername, false, "already exists", nil)
}
