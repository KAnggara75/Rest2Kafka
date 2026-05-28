package handler

import "net/http"

// RegisterRoutes initializes a new http.ServeMux and registers all handlers.
func (h *Handler) RegisterRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/publish/{clusterName}/{topic}", h.HandlePublish)
	mux.HandleFunc("GET /api/v1/clusters", h.HandleListClusters)
	mux.HandleFunc("GET /api/v1/{clusterName}/topic", h.HandleListTopics)
	mux.HandleFunc("GET /health", h.HandleHealth)
	return mux
}
