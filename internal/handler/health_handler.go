package handler

import (
	"net/http"

	"github.com/KAnggara75/Rest2Kafka/internal/model"
)

// HandleHealth handles health check requests.
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, model.HealthResponse{
		Status: "UP",
	})
}
