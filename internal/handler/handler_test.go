package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/KAnggara75/Rest2Kafka/internal/config"
	"github.com/KAnggara75/Rest2Kafka/internal/model"
)

var testAuthCfg = config.AuthConfig{
	LoginUsername: "admin",
	LoginPassword: "admin",
	JWTSecret:     "kafkadesk-secret-key-2026",
}

func getTestToken() string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "admin",
		"exp": time.Now().Add(24 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})
	tokenString, _ := token.SignedString([]byte(testAuthCfg.JWTSecret))
	return "Bearer " + tokenString
}

type MockPublishService struct {
	PublishFunc      func(ctx context.Context, clusterName, topic, key, value string) error
	ListClustersFunc func() []model.ClusterDetail
	ListTopicsFunc   func(ctx context.Context, clusterName string) ([]string, error)
}

func (m *MockPublishService) Publish(ctx context.Context, clusterName, topic, key, value string) error {
	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, clusterName, topic, key, value)
	}
	return nil
}

func (m *MockPublishService) ListClusters() []model.ClusterDetail {
	if m.ListClustersFunc != nil {
		return m.ListClustersFunc()
	}
	return nil
}

func (m *MockPublishService) ListTopics(ctx context.Context, clusterName string) ([]string, error) {
	if m.ListTopicsFunc != nil {
		return m.ListTopicsFunc(ctx, clusterName)
	}
	return nil, nil
}

func TestHandlePublish_Success(t *testing.T) {
	mockSvc := &MockPublishService{
		PublishFunc: func(ctx context.Context, clusterName, topic, key, value string) error {
			if clusterName != "c1" || topic != "t1" || key != "k1" || value != "v1" {
				return errors.New("unexpected params")
			}
			return nil
		},
	}

	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	body, _ := json.Marshal(model.PublishRequest{
		Key:   "k1",
		Value: "v1",
	})
	req := httptest.NewRequest("POST", "/api/v1/publish/c1/t1", bytes.NewReader(body))
	req.Header.Set("Authorization", getTestToken())
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}

	var resp model.PublishResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("expected success status, got %s", resp.Status)
	}
}

func TestHandlePublish_MissingValue(t *testing.T) {
	mockSvc := &MockPublishService{}
	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	body, _ := json.Marshal(model.PublishRequest{
		Key: "k1",
	})
	req := httptest.NewRequest("POST", "/api/v1/publish/c1/t1", bytes.NewReader(body))
	req.Header.Set("Authorization", getTestToken())
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", w.Code)
	}
}

func TestHandleListClusters(t *testing.T) {
	mockSvc := &MockPublishService{
		ListClustersFunc: func() []model.ClusterDetail {
			return []model.ClusterDetail{
				{Name: "c1", Brokers: []string{"b1", "b2"}},
			}
		},
	}

	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	req := httptest.NewRequest("GET", "/api/v1/clusters", nil)
	req.Header.Set("Authorization", getTestToken())
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}

	var resp model.ListClustersResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Clusters) != 1 || resp.Clusters[0].Name != "c1" || resp.Clusters[0].Brokers[0] != "b1" {
		t.Errorf("unexpected clusters list: %v", resp.Clusters)
	}
}

func TestHandleListTopics(t *testing.T) {
	mockSvc := &MockPublishService{
		ListTopicsFunc: func(ctx context.Context, clusterName string) ([]string, error) {
			if clusterName != "dev" {
				return nil, errors.New("unexpected cluster")
			}
			return []string{"t1", "t2"}, nil
		},
	}

	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	req := httptest.NewRequest("GET", "/api/v1/dev/topic", nil)
	req.Header.Set("Authorization", getTestToken())
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}

	var resp model.ListTopicsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Topics) != 2 || resp.Topics[0] != "t1" || resp.Topics[1] != "t2" {
		t.Errorf("unexpected topics list: %v", resp.Topics)
	}
}

func TestHandleLogin_Success(t *testing.T) {
	mockSvc := &MockPublishService{}
	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	body, _ := json.Marshal(model.LoginRequest{
		Username: "admin",
		Password: "admin",
	})
	req := httptest.NewRequest("POST", "/api/v1/login", bytes.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}

	var resp model.LoginResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Token == "" {
		t.Error("expected non-empty token")
	}
}

func TestHandleLogin_Failure(t *testing.T) {
	mockSvc := &MockPublishService{}
	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	body, _ := json.Marshal(model.LoginRequest{
		Username: "admin",
		Password: "wrongpassword",
	})
	req := httptest.NewRequest("POST", "/api/v1/login", bytes.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", w.Code)
	}
}

func TestJWTMiddleware_NoAuthHeader(t *testing.T) {
	mockSvc := &MockPublishService{}
	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	req := httptest.NewRequest("GET", "/api/v1/clusters", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", w.Code)
	}
}

func TestJWTMiddleware_InvalidToken(t *testing.T) {
	mockSvc := &MockPublishService{}
	h := NewHandler(mockSvc, testAuthCfg)
	mux := h.RegisterRoutes()

	req := httptest.NewRequest("GET", "/api/v1/clusters", nil)
	req.Header.Set("Authorization", "Bearer invalidtoken")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", w.Code)
	}
}
