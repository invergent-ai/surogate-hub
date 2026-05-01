package esti

import (
	"strconv"
	"testing"

	"github.com/spf13/viper"
)

func HubWithParams(connectionString string) string {
	return HubWithParamsWithBasicAuth(connectionString, false)
}

func HubWithParamsWithBasicAuth(connectionString string, basicAuth bool) string {
	sghubCmdline := "SGHUB_DATABASE_TYPE=postgres" +
		" SGHUB_DATABASE_POSTGRES_CONNECTION_STRING=" + connectionString +
		" SGHUB_AUTH_INTERNAL_BASIC=" + strconv.FormatBool(basicAuth) +
		" SGHUB_BLOCKSTORE_TYPE=" + viper.GetString("blockstore_type") +
		" SGHUB_AUTH_ENCRYPT_SECRET_KEY='some random secret string' " + hubLocation()

	return sghubCmdline
}

func hubLocation() string {
	return viper.GetString("binaries_dir") + "/sghub"
}

func HubWithBasicAuth() string {
	return HubWithParamsWithBasicAuth(viper.GetString("database_connection_string"), true)
}

func SgHub() string {
	return HubWithParams(viper.GetString("database_connection_string"))
}

func RequirePostgresDB(t *testing.T) {
	dbString := viper.GetString("database_connection_string")
	if dbString == "" {
		t.Skip("skip test - not postgres")
	}
}
