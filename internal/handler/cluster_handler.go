package handler

import (
	"net/http"

	"github.com/rs/zerolog/log"

	"github.com/KAnggara75/Rest2Kafka/internal/model"
)

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
