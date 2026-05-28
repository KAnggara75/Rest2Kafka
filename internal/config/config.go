package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type ServerConfig struct {
	Port                int
	ReadTimeoutSeconds  int
	WriteTimeoutSeconds int
}

type ClusterConfig struct {
	Brokers               []string
	SASLMechanism         string
	SecurityProtocol      string
	SASLJaasConfig        string
	RequestTimeoutMs      int
	SSLTruststoreLocation string
	SSLTruststorePassword string
	SSLCALocation         string
}

type AuthConfig struct {
	LoginUsername string
	LoginPassword string
	JWTSecret     string
}

type Config struct {
	Server   ServerConfig
	Clusters map[string]ClusterConfig
	Auth     AuthConfig
}

func LoadConfig(envPath string) (*Config, error) {
	if envPath == "" {
		envPath = ".env"
	}
	_ = godotenv.Load(envPath)

	var cfg Config

	// 1. Parse Server Configuration
	portStr := os.Getenv("PORT")
	if portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid PORT env var: %w", err)
		}
		cfg.Server.Port = port
	} else {
		cfg.Server.Port = 8080
	}

	readTimeoutStr := os.Getenv("READ_TIMEOUT_SECONDS")
	if readTimeoutStr != "" {
		timeout, err := strconv.Atoi(readTimeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid READ_TIMEOUT_SECONDS env var: %w", err)
		}
		cfg.Server.ReadTimeoutSeconds = timeout
	} else {
		cfg.Server.ReadTimeoutSeconds = 15
	}

	writeTimeoutStr := os.Getenv("WRITE_TIMEOUT_SECONDS")
	if writeTimeoutStr != "" {
		timeout, err := strconv.Atoi(writeTimeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid WRITE_TIMEOUT_SECONDS env var: %w", err)
		}
		cfg.Server.WriteTimeoutSeconds = timeout
	} else {
		cfg.Server.WriteTimeoutSeconds = 15
	}

	// 2. Parse Kafka Clusters Configuration using indexed env format
	cfg.Clusters = make(map[string]ClusterConfig)
	for i := 0; ; i++ {
		nameKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_NAME", i)
		name := cleanQuotes(os.Getenv(nameKey))
		if name == "" {
			break
		}

		brokersKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_BOOTSTRAPSERVERS", i)
		brokersVal := cleanQuotes(os.Getenv(brokersKey))
		var brokers []string
		if brokersVal != "" {
			brokers = splitAndClean(brokersVal, ",")
		}

		saslMechanismKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SASL_MECHANISM", i)
		saslMechanism := cleanQuotes(os.Getenv(saslMechanismKey))

		securityProtocolKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SECURITY_PROTOCOL", i)
		securityProtocol := cleanQuotes(os.Getenv(securityProtocolKey))

		saslJaasConfigKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SASL_JAAS_CONFIG", i)
		saslJaasConfig := cleanQuotes(os.Getenv(saslJaasConfigKey))

		requestTimeoutKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_REQUEST_TIMEOUT_MS", i)
		requestTimeoutVal := cleanQuotes(os.Getenv(requestTimeoutKey))
		var requestTimeout int
		if requestTimeoutVal != "" {
			requestTimeout, _ = strconv.Atoi(requestTimeoutVal)
		}

		sslTruststoreLocationKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SSL_TRUSTSTORE_LOCATION", i)
		sslTruststoreLocation := cleanQuotes(os.Getenv(sslTruststoreLocationKey))

		sslTruststorePasswordKey1 := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SSL_TRUSTSTORE_PASSWORD", i)
		sslTruststorePasswordKey2 := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SSL_TRUSTSTORE_PASSWOR", i)
		sslTruststorePassword := cleanQuotes(os.Getenv(sslTruststorePasswordKey1))
		if sslTruststorePassword == "" {
			sslTruststorePassword = cleanQuotes(os.Getenv(sslTruststorePasswordKey2))
		}

		sslCALocationKey := fmt.Sprintf("KAFKA_CLUSTERS_%d_PROPERTIES_SSL_CA_LOCATION", i)
		sslCALocation := cleanQuotes(os.Getenv(sslCALocationKey))

		cfg.Clusters[name] = ClusterConfig{
			Brokers:               brokers,
			SASLMechanism:         saslMechanism,
			SecurityProtocol:      securityProtocol,
			SASLJaasConfig:        saslJaasConfig,
			RequestTimeoutMs:      requestTimeout,
			SSLTruststoreLocation: sslTruststoreLocation,
			SSLTruststorePassword: sslTruststorePassword,
			SSLCALocation:         sslCALocation,
		}
	}

	// 3. Parse Auth Configuration
	loginUsername := cleanQuotes(os.Getenv("LOGIN_USERNAME"))
	if loginUsername == "" {
		loginUsername = "admin"
	}
	loginPassword := cleanQuotes(os.Getenv("LOGIN_PASSWORD"))
	if loginPassword == "" {
		loginPassword = "admin"
	}
	jwtSecret := cleanQuotes(os.Getenv("JWT_SECRET"))
	if jwtSecret == "" {
		jwtSecret = "kafkadesk-secret-key-2026"
	}

	cfg.Auth = AuthConfig{
		LoginUsername: loginUsername,
		LoginPassword: loginPassword,
		JWTSecret:     jwtSecret,
	}

	return &cfg, nil
}

func cleanQuotes(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"'`)
	return strings.TrimSpace(s)
}

func splitAndClean(s, sep string) []string {
	parts := strings.Split(s, sep)
	var res []string
	for _, p := range parts {
		cleaned := cleanQuotes(p)
		if cleaned != "" {
			res = append(res, cleaned)
		}
	}
	return res
}
