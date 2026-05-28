package handler

import "net/http"

// RegisterRoutes initializes a new http.ServeMux and registers all handlers.
func (h *Handler) RegisterRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Unprotected routes
	mux.HandleFunc("GET /health", h.HandleHealth)
	mux.HandleFunc("POST /api/v1/login", h.HandleLogin)

	// Protected routes
	mux.Handle("POST /api/v1/publish/{clusterName}/{topic}", h.JWTMiddleware(http.HandlerFunc(h.HandlePublish)))
	mux.Handle("GET /api/v1/clusters", h.JWTMiddleware(http.HandlerFunc(h.HandleListClusters)))
	mux.Handle("GET /api/v1/{clusterName}/topic", h.JWTMiddleware(http.HandlerFunc(h.HandleListTopics)))

	return mux
}
