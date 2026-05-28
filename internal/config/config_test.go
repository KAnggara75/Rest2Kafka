package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	content := []byte(`PORT=9090
READ_TIMEOUT_SECONDS=10
WRITE_TIMEOUT_SECONDS=20
KAFKA_CLUSTERS_0_NAME="c1"
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS="b1,b2"
KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM="PLAIN"
KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL="SASL_SSL"
KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";"
KAFKA_CLUSTERS_0_PROPERTIES_REQUEST_TIMEOUT_MS="60000"
KAFKA_CLUSTERS_0_PROPERTIES_SSL_TRUSTSTORE_LOCATION="/opt/truststore.jks"
KAFKA_CLUSTERS_0_PROPERTIES_SSL_TRUSTSTORE_PASSWORD="pass"
`)
	tmpFile, err := os.CreateTemp("", "*.env")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	// Clear environment variables before loading to prevent conflicts
	os.Unsetenv("PORT")
	os.Unsetenv("READ_TIMEOUT_SECONDS")
	os.Unsetenv("WRITE_TIMEOUT_SECONDS")
	os.Unsetenv("KAFKA_CLUSTERS_0_NAME")
	os.Unsetenv("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS")
	os.Unsetenv("KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM")
	os.Unsetenv("KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL")
	os.Unsetenv("KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG")
	os.Unsetenv("KAFKA_CLUSTERS_0_PROPERTIES_REQUEST_TIMEOUT_MS")
	os.Unsetenv("KAFKA_CLUSTERS_0_PROPERTIES_SSL_TRUSTSTORE_LOCATION")
	os.Unsetenv("KAFKA_CLUSTERS_0_PROPERTIES_SSL_TRUSTSTORE_PASSWORD")

	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Server.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeoutSeconds != 10 {
		t.Errorf("expected read timeout 10, got %d", cfg.Server.ReadTimeoutSeconds)
	}
	if cfg.Server.WriteTimeoutSeconds != 20 {
		t.Errorf("expected write timeout 20, got %d", cfg.Server.WriteTimeoutSeconds)
	}

	c1, ok := cfg.Clusters["c1"]
	if !ok {
		t.Fatalf("expected cluster c1 to exist")
	}
	if len(c1.Brokers) != 2 || c1.Brokers[0] != "b1" || c1.Brokers[1] != "b2" {
		t.Errorf("unexpected brokers for c1: %v", c1.Brokers)
	}
	if c1.SASLMechanism != "PLAIN" {
		t.Errorf("expected SASLMechanism PLAIN, got %s", c1.SASLMechanism)
	}
	if c1.SecurityProtocol != "SASL_SSL" {
		t.Errorf("expected SecurityProtocol SASL_SSL, got %s", c1.SecurityProtocol)
	}
	if !strings.Contains(c1.SASLJaasConfig, `username="u"`) {
		t.Errorf("unexpected SASLJaasConfig: %s", c1.SASLJaasConfig)
	}
	if c1.RequestTimeoutMs != 60000 {
		t.Errorf("expected RequestTimeoutMs 60000, got %d", c1.RequestTimeoutMs)
	}
	if c1.SSLTruststoreLocation != "/opt/truststore.jks" {
		t.Errorf("expected SSLTruststoreLocation /opt/truststore.jks, got %s", c1.SSLTruststoreLocation)
	}
	if c1.SSLTruststorePassword != "pass" {
		t.Errorf("expected SSLTruststorePassword pass, got %s", c1.SSLTruststorePassword)
	}
}
