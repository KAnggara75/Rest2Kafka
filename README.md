# Kafka Multi-Cluster Publish Service

- **Code Coverage**: 26.8% (Target: 10%)

A Go-based REST API that enables publishing messages to multiple Kafka clusters dynamically. It supports structured logging with `zerolog`, hot connection caching, SASL authentication (PLAIN/SCRAM), and SSL/TLS configuration including Java Keystore (JKS) truststore loading or remote PEM certificate downloading.

---

## Features

- **Multi-Cluster Support**: Map dynamically configured Kafka clusters via index-based environment variables.
- **REST API Endpoint**: Trigger publications dynamically via a unified endpoint structure.
- **Zero CGO Dependency**: Built using `github.com/segmentio/kafka-go` (pure Go implementation).
- **Fast Structured JSON Logging**: Complete logging using `github.com/rs/zerolog` with custom level configurations.
- **Robust TLS/SSL Configuration**:
  - Load Java Keystore (`.jks`) truststores dynamically.
  - Load PEM certificates from local paths or download them dynamically from remote HTTPS URLs (e.g. Aiven custom CAs).
- **SASL Authentication**: Parses standard JAAS configurations to support `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` mechanisms.
- **Graceful Shutdown**: Automatically closes client writers and terminates connection pools upon receiving OS signals.

---

## Configuration

Configurations are loaded from a `.env` file at the root of the project.

### `.env` File Example
Create a `.env` file containing:

```env
PORT=8080
READ_TIMEOUT_SECONDS=15
WRITE_TIMEOUT_SECONDS=15
LOG_LEVEL=info

# Kafka Cluster 0 Configuration (e.g., dev cluster)
KAFKA_CLUSTERS_0_NAME="dev"
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS="kafka-dev-kanggara75.c.aivencloud.com:24227"
KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM="PLAIN"
KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL="SASL_SSL"
KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my-username\" password=\"my-password\";"
KAFKA_CLUSTERS_0_PROPERTIES_REQUEST_TIMEOUT_MS="60000"
KAFKA_CLUSTERS_0_PROPERTIES_DEFAULT_API_TIMEOUT_MS="60000"
KAFKA_CLUSTERS_0_PROPERTIES_SOCKET_CONNECTION_SETUP_TIMEOUT_MS="60000"
KAFKA_CLUSTERS_0_PROPERTIES_SSL_CA_LOCATION="https://gist.githubusercontent.com/KAnggara75/3cd04916f8f0719f924b234f15d89fb2/raw/9b9e8e0d1e3a044a04e11b7fc072be094d7d267d/kafka-dev-kanggara75.pem"

# Kafka Cluster 1 Configuration (e.g., staging cluster using JKS)
KAFKA_CLUSTERS_1_NAME="staging"
KAFKA_CLUSTERS_1_BOOTSTRAPSERVERS="staging-broker1:9092,staging-broker2:9092"
KAFKA_CLUSTERS_1_PROPERTIES_SECURITY_PROTOCOL="SSL"
KAFKA_CLUSTERS_1_PROPERTIES_SSL_TRUSTSTORE_LOCATION="/opt/staging-truststore.jks"
KAFKA_CLUSTERS_1_PROPERTIES_SSL_TRUSTSTORE_PASSWORD="my-truststore-password"
```

---

## API Endpoints

### 1. Health Check
Checks if the HTTP API service is running.

- **Method**: `GET`
- **Path**: `/health`
- **Response**: `200 OK`
  ```json
  {
    "status": "UP"
  }
  ```

#### Example `curl`:
```bash
curl -i http://localhost:8080/health
```

---

### 2. Get Configured Clusters
Retrieves a list of configured Kafka cluster names and their broker addresses.

- **Method**: `GET`
- **Path**: `/api/v1/clusters`
- **Response**: `200 OK`
  ```json
  {
    "clusters": [
      {
        "name": "dev",
        "brokers": ["kafka-dev-kanggara75.c.aivencloud.com:24227"]
      }
    ]
  }
  ```

#### Example `curl`:
```bash
curl -i http://localhost:8080/api/v1/clusters
```

---

### 3. Get Cluster Topics
Retrieves a list of all available topics in a specific Kafka cluster.

- **Method**: `GET`
- **Path**: `/api/v1/{clusterName}/topic`
  - `clusterName`: The name configured in the environment variables (e.g. `dev`, `pakaiwa`).
- **Response**: `200 OK`
  ```json
  {
    "topics": [
      "__consumer_offsets",
      "_schemas",
      "pakaiwa-delivery-status"
    ]
  }
  ```

#### Example `curl`:
```bash
curl -i http://localhost:8080/api/v1/dev/topic
```

---

### 4. Publish Message
Publishes a message to a specific cluster and topic.

- **Method**: `POST`
- **Path**: `/api/v1/publish/{clusterName}/{topic}`
  - `clusterName`: The name configured in the environment variables (e.g. `dev`, `staging`).
  - `topic`: The destination Kafka topic name.
- **Request Headers**:
  - `Content-Type: application/json`
- **Request Body (JSON)**:
  - `key` (string, optional): Partition routing key.
  - `value` (string, required): Message value payload.
- **Response**:
  - `200 OK`
    ```json
    {
      "status": "success",
      "message": "Message published successfully"
    }
    ```
  - `400 Bad Request` (Invalid inputs / payload)
    ```json
    {
      "status": "error",
      "message": "Value is required"
    }
    ```
  - `500 Internal Server Error` (Broker connection error / unconfigured cluster)
    ```json
    {
      "status": "error",
      "message": "cluster \"nonexistent\" is not defined in configuration"
    }
    ```

#### Example `curl`:
```bash
curl -i -X POST \
  -H "Content-Type: application/json" \
  -d '{"key": "user-login-event", "value": "{\"user_id\": 12345, \"status\": \"success\"}"}' \
  http://localhost:8080/api/v1/publish/dev/user-events-topic
```

---

## Getting Started

### 1. Pre-requisites
Make sure you have Go installed:
```bash
go version
```

### 2. Install Dependencies
Download and tidy dependencies:
```bash
go mod tidy
```

### 3. Run Unit Tests
Verify and run tests:
```bash
go test ./...
```

### 4. Run the Service
Start the HTTP API server:
```bash
go run cmd/rest2kafka/main.go
```
The server will boot and begin listening on the port configured in `.env` (default is `:8888`).

---

## Directory Structure

This project follows the standard Go project layout:

```text
rest2kafka/
├── cmd/
│   └── rest2kafka/
│       └── main.go       # Application entrypoint
├── internal/
│   ├── config/           # Configuration structs & parser
│   ├── handler/          # HTTP API handlers & middleware
│   ├── kafka/            # Low-level Kafka connections & writers
│   ├── model/            # Shared DTOs & response/request structs
│   └── service/          # Business service layer
├── .env                  # Local configurations (git-ignored)
└── .gitignore            # Git exclusion rules
```
