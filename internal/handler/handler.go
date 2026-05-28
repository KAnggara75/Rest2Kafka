package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/KAnggara75/Rest2Kafka/internal/config"
	"github.com/KAnggara75/Rest2Kafka/internal/model"
	"github.com/KAnggara75/Rest2Kafka/internal/service"

	"github.com/rs/zerolog/log"
)

type Handler struct {
	svc       service.PublishService
	authCfg   config.AuthConfig
	blacklist *TokenBlacklist
}

func NewHandler(svc service.PublishService, authCfg config.AuthConfig) *Handler {
	return &Handler{
		svc:       svc,
		authCfg:   authCfg,
		blacklist: NewTokenBlacklist(),
	}
}

// HandlePublish handles HTTP POST requests to publish to a topic.
func (h *Handler) HandlePublish(w http.ResponseWriter, r *http.Request) {
	clusterName := r.PathValue("clusterName")
	topic := r.PathValue("topic")

	if clusterName == "" || topic == "" {
		h.writeJSON(w, http.StatusBadRequest, model.PublishResponse{
			Status:  "error",
			Message: "Missing clusterName or topic parameter",
		})
		return
	}

	var req model.PublishRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		h.writeJSON(w, http.StatusBadRequest, model.PublishResponse{
			Status:  "error",
			Message: "Invalid request payload",
		})
		return
	}

	if req.Value == "" {
		h.writeJSON(w, http.StatusBadRequest, model.PublishResponse{
			Status:  "error",
			Message: "Value is required",
		})
		return
	}

	log.Info().
		Str("cluster", clusterName).
		Str("topic", topic).
		Str("key", req.Key).
		Msg("Publishing message")

	ctx := r.Context()
	err := h.svc.Publish(ctx, clusterName, topic, req.Key, req.Value)
	if err != nil {
		log.Error().Err(err).Msg("Error publishing message")
		h.writeJSON(w, http.StatusInternalServerError, model.PublishResponse{
			Status:  "error",
			Message: err.Error(),
		})
		return
	}

	h.writeJSON(w, http.StatusOK, model.PublishResponse{
		Status:  "success",
		Message: "Message published successfully",
	})
}

// HandleHealth handles health check requests.
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, model.HealthResponse{
		Status: "UP",
	})
}

// HandleListClusters handles HTTP GET requests to list all configured clusters.
func (h *Handler) HandleListClusters(w http.ResponseWriter, r *http.Request) {
	clusters := h.svc.ListClusters()
	h.writeJSON(w, http.StatusOK, model.ListClustersResponse{
		Clusters: clusters,
	})
}

// HandleListTopics handles HTTP GET requests to list all topics on a specific cluster.
func (h *Handler) HandleListTopics(w http.ResponseWriter, r *http.Request) {
	clusterName := r.PathValue("clusterName")
	if clusterName == "" {
		h.writeJSON(w, http.StatusBadRequest, model.PublishResponse{
			Status:  "error",
			Message: "Missing clusterName parameter",
		})
		return
	}

	ctx := r.Context()
	topics, err := h.svc.ListTopics(ctx, clusterName)
	if err != nil {
		log.Error().Err(err).Msg("Error listing topics")
		h.writeJSON(w, http.StatusInternalServerError, model.PublishResponse{
			Status:  "error",
			Message: err.Error(),
		})
		return
	}

	h.writeJSON(w, http.StatusOK, model.ListTopicsResponse{
		Topics: topics,
	})
}

func (h *Handler) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Error().Err(err).Msg("Error encoding response")
	}
}

// LoggingMiddleware logs detailed incoming HTTP requests and durations using zerolog.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		wrapped := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", wrapped.statusCode).
			Dur("duration", duration).
			Str("client", r.RemoteAddr).
			Msg("HTTP Request")
	})
}

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterWrapper) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
