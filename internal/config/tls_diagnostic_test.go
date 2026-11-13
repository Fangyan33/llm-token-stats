package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func TestTLSFailuresIdentifyCertificateOrPrivateKey(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	encode := func(key ed25519.PrivateKey) []byte {
		t.Helper()
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	}
	_, other, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, field, reason string
		cert, key           []byte
	}{
		{"certificate", "kafka.cert_file", "invalid_certificate_pem", []byte("private-test-secret"), encode(key)},
		{"key", "kafka.key_file", "invalid_private_key", cert, []byte("private-test-secret")},
		{"mismatch", "kafka.key_file", "certificate_key_mismatch", cert, encode(other)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			certFile, keyFile := filepath.Join(dir, "certificate.pem"), filepath.Join(dir, "key.pem")
			if err := os.WriteFile(certFile, tc.cert, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(keyFile, tc.key, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := (Kafka{CertFile: certFile, KeyFile: keyFile}).TLS()
			r := configdiag.Find(err)
			if r == nil || r.Issues[0].Field != tc.field || r.Issues[0].Reason != tc.reason {
				t.Fatalf("wrong certificate/key diagnostic: %+v", r)
			}
			if strings.Contains(strings.Join(r.Issues[0].Fields(), " "), "private-test-secret") {
				t.Fatal("private key contents leaked")
			}
		})
	}
}
