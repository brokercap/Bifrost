//go:build integration
// +build integration

package mysql_tls

import (
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"os"
	"testing"
)

func TestMySQLTLS(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	caCert := pwd + "/tls/ca-crt.pem"
	clientCert := pwd + "/tls/client-cert.pem"
	clientKey := pwd + "/tls/client-key.pem"
	uri := "root:123456@tcp(127.0.0.1:53306)/mysql?ca-cert=" + caCert + "&client-cert=" + clientCert + "&client-key=" + clientKey + "&insecure-skip-verify=false&servername=server.mysql.test.com"
	conn := mysql.NewConnect(uri)
	err = conn.Ping()
	if err != nil {
		t.Fatal(err)
	}
}
