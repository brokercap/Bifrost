package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

func NewClientTLSConfigWithFile(caPemFile, certPemFile, keyPemFile string, insecureSkipVerify bool, serverName string) (tlsConfig *tls.Config, err error) {
	var caPem, certPem, keyPem []byte
	caPem, err = os.ReadFile(caPemFile)
	if err != nil {
		return
	}
	if certPemFile != "" {
		certPem, err = os.ReadFile(certPemFile)
		if err != nil {
			return
		}
	}
	if keyPemFile != "" {
		keyPem, err = os.ReadFile(keyPemFile)
		if err != nil {
			return
		}
	}
	return NewClientTLSConfig(caPem, certPem, keyPem, insecureSkipVerify, serverName)
}

func NewClientTLSConfig(caPem, certPem, keyPem []byte, insecureSkipVerify bool, serverName string) (tlsConfig *tls.Config, err error) {
	if len(caPem) == 0 {
		err = errors.New("caPem is empty")
		return
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		err = errors.New("failed to add ca PEM")
		return
	}

	// Allow cert and key to be optional
	// Send through `make([]byte, 0)` for "nil"
	if string(certPem) != "" && string(keyPem) != "" {
		var cert tls.Certificate
		cert, err = tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			return
		}
		tlsConfig = &tls.Config{
			RootCAs:            pool,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: insecureSkipVerify,
			ServerName:         serverName,
		}
	} else {
		tlsConfig = &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: insecureSkipVerify,
			ServerName:         serverName,
		}
	}

	return
}
