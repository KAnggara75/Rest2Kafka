package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/KAnggara75/Rest2Kafka/internal/config"
	"github.com/KAnggara75/Rest2Kafka/internal/model"

	"github.com/pavlo-v-chernykh/keystore-go/v4"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Manager struct {
	mu      sync.RWMutex
	writers map[string]*kafka.Writer
	config  *config.Config
}

func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		writers: make(map[string]*kafka.Writer),
		config:  cfg,
	}
}

// ListClusters returns a list of configured cluster names and their brokers.
func (m *Manager) ListClusters() []model.ClusterDetail {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []model.ClusterDetail
	for name, clusterCfg := range m.config.Clusters {
		list = append(list, model.ClusterDetail{
			Name:    name,
			Brokers: clusterCfg.Brokers,
		})
	}
	return list
}

// ListTopics fetches all available topics on the specified Kafka cluster.
func (m *Manager) ListTopics(ctx context.Context, clusterName string) ([]string, error) {
	clusterCfg, ok := m.config.Clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("cluster %q is not defined in configuration", clusterName)
	}

	// We reuse GetWriter to initialize the connection pool & SASL/TLS transport
	writer, err := m.GetWriter(clusterName)
	if err != nil {
		return nil, err
	}

	client := &kafka.Client{
		Addr:      kafka.TCP(clusterCfg.Brokers...),
		Transport: writer.Transport,
	}

	log.Info().Str("cluster", clusterName).Msg("Fetching metadata to list topics")

	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	var topics []string
	for _, t := range resp.Topics {
		if strings.HasPrefix(t.Name, "_") {
			continue // Skip internal topics (e.g. __consumer_offsets, _schemas)
		}
		topics = append(topics, t.Name)
	}

	log.Info().
		Str("cluster", clusterName).
		Int("count", len(topics)).
		Msg("Successfully fetched topic list")

	return topics, nil
}

// GetWriter gets or creates a kafka.Writer for a specific cluster.
func (m *Manager) GetWriter(clusterName string) (*kafka.Writer, error) {
	m.mu.RLock()
	writer, exists := m.writers[clusterName]
	m.mu.RUnlock()

	if exists {
		return writer, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check under write lock
	if writer, exists = m.writers[clusterName]; exists {
		return writer, nil
	}

	clusterCfg, ok := m.config.Clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("cluster %q is not defined in configuration", clusterName)
	}

	if len(clusterCfg.Brokers) == 0 {
		return nil, fmt.Errorf("cluster %q has no brokers configured", clusterName)
	}

	// 1. Configure SASL Mechanism if provided
	var saslMech sasl.Mechanism
	if clusterCfg.SASLMechanism != "" && clusterCfg.SASLJaasConfig != "" {
		user, pass := parseJAASConfig(clusterCfg.SASLJaasConfig)
		if user == "" || pass == "" {
			return nil, fmt.Errorf("failed to parse username/password from JAAS config for cluster %q", clusterName)
		}

		mech := strings.ToUpper(clusterCfg.SASLMechanism)
		switch mech {
		case "PLAIN":
			saslMech = plain.Mechanism{
				Username: user,
				Password: pass,
			}
		case "SCRAM-SHA-256":
			var err error
			saslMech, err = scram.Mechanism(scram.SHA256, user, pass)
			if err != nil {
				return nil, fmt.Errorf("failed to create SCRAM-SHA-256 mechanism: %w", err)
			}
		case "SCRAM-SHA-512":
			var err error
			saslMech, err = scram.Mechanism(scram.SHA512, user, pass)
			if err != nil {
				return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism %q for cluster %q", clusterCfg.SASLMechanism, clusterName)
		}
	}

	// 2. Configure TLS/SSL if requested
	var tlsConfig *tls.Config
	proto := strings.ToUpper(clusterCfg.SecurityProtocol)
	if proto == "SASL_SSL" || proto == "SSL" {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if clusterCfg.SSLTruststoreLocation != "" {
			rootCAs, err := loadTruststore(clusterCfg.SSLTruststoreLocation, clusterCfg.SSLTruststorePassword)
			if err != nil {
				return nil, fmt.Errorf("failed to load SSL truststore: %w", err)
			}
			tlsConfig.RootCAs = rootCAs
		} else if clusterCfg.SSLCALocation != "" {
			rootCAs, err := loadCACert(clusterCfg.SSLCALocation)
			if err != nil {
				return nil, fmt.Errorf("failed to load SSL CA certificate: %w", err)
			}
			tlsConfig.RootCAs = rootCAs
		}
	}

	// Create custom transport if SASL or TLS is required
	var transport *kafka.Transport
	if saslMech != nil || tlsConfig != nil {
		transport = &kafka.Transport{
			SASL: saslMech,
			TLS:  tlsConfig,
		}
	}

	writeTimeout := 10 * time.Second
	if clusterCfg.RequestTimeoutMs > 0 {
		writeTimeout = time.Duration(clusterCfg.RequestTimeoutMs) * time.Millisecond
	}

	log.Info().
		Str("cluster", clusterName).
		Interface("brokers", clusterCfg.Brokers).
		Str("saslMechanism", clusterCfg.SASLMechanism).
		Bool("tls", tlsConfig != nil).
		Msg("Initializing Kafka Writer")

	// Create writer for this cluster. Note: we do not set a Topic here,
	// allowing this writer to send messages to any topic by setting
	// the Topic field in kafka.Message.
	w := &kafka.Writer{
		Addr:         kafka.TCP(clusterCfg.Brokers...),
		Balancer:     &kafka.Hash{},
		MaxAttempts:  3,
		WriteTimeout: writeTimeout,
		ReadTimeout:  10 * time.Second,
	}
	if transport != nil {
		w.Transport = transport
	}

	m.writers[clusterName] = w
	return w, nil
}

// Publish sends a message to the specified cluster and topic.
func (m *Manager) Publish(ctx context.Context, clusterName, topic, key, value string) error {
	writer, err := m.GetWriter(clusterName)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	}

	start := time.Now()
	if err := writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to cluster %q, topic %q: %w", clusterName, topic, err)
	}
	duration := time.Since(start)

	log.Info().
		Str("cluster", clusterName).
		Str("topic", topic).
		Int("partition", msg.Partition).
		Int64("offset", msg.Offset).
		Str("key", key).
		Dur("elapsed", duration).
		Msg("Successfully published message to Kafka")

	return nil
}

// Close closes all open writers.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for name, writer := range m.writers {
		log.Info().Str("cluster", name).Msg("Closing Kafka Writer")
		if err := writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close writer for cluster %q: %w", name, err))
		}
	}
	m.writers = make(map[string]*kafka.Writer)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing writers: %v", errs)
	}
	return nil
}

// Helper: parseJAASConfig extracts username and password from standard JAAS format.
func parseJAASConfig(jaas string) (username, password string) {
	userRegex := regexp.MustCompile(`username=["']([^"']+)["']`)
	passRegex := regexp.MustCompile(`password=["']([^"']+)["']`)

	userMatches := userRegex.FindStringSubmatch(jaas)
	passMatches := passRegex.FindStringSubmatch(jaas)

	if len(userMatches) > 1 {
		username = userMatches[1]
	}
	if len(passMatches) > 1 {
		password = passMatches[1]
	}
	return
}

// Helper: loadTruststore loads a JKS truststore file and extracts its CA certificates.
func loadTruststore(path, password string) (*x509.CertPool, error) {
	// #nosec G304
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open truststore file: %w", err)
	}
	defer f.Close()

	lk := keystore.New()
	err = lk.Load(f, []byte(password))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Java Keystore: %w", err)
	}

	certPool := x509.NewCertPool()
	added := 0
	for _, alias := range lk.Aliases() {
		if lk.IsTrustedCertificateEntry(alias) {
			certEntry, err := lk.GetTrustedCertificateEntry(alias)
			if err != nil {
				continue
			}
			cert, err := x509.ParseCertificate(certEntry.Certificate.Content)
			if err == nil {
				certPool.AddCert(cert)
				added++
			}
		}
	}

	if added == 0 {
		return nil, fmt.Errorf("no trusted certificates found in keystore")
	}

	return certPool, nil
}

// Helper: loadCACert loads CA PEM certificates from local path or remote URL.
func loadCACert(location string) (*x509.CertPool, error) {
	var pemData []byte
	var err error

	if strings.HasPrefix(location, "http://") || strings.HasPrefix(location, "https://") {
		log.Info().Str("url", location).Msg("Downloading CA certificate")
		// #nosec G107
		resp, err := http.Get(location)
		if err != nil {
			return nil, fmt.Errorf("failed to download CA cert from URL %q: %w", location, err)
		}
		defer resp.Body.Close()

		pemData, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read downloaded CA cert body: %w", err)
		}
	} else {
		log.Info().Str("path", location).Msg("Reading CA certificate from local file")
		// #nosec G304
		pemData, err = os.ReadFile(location)
		if err != nil {
			return nil, fmt.Errorf("failed to read local CA cert file: %w", err)
		}
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("failed to parse PEM CA certificate from location %q", location)
	}

	log.Info().Str("location", location).Msg("Successfully loaded CA certificate")
	return certPool, nil
}
