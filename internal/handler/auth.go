package handler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/KAnggara75/Rest2Kafka/internal/model"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog/log"
)

// HandleLogin authenticates username and password and returns a JWT token.
func (h *Handler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	var req model.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSON(w, http.StatusBadRequest, model.PublishResponse{
			Status:  "error",
			Message: "Invalid request payload",
		})
		return
	}

	if req.Username != h.authCfg.LoginUsername || req.Password != h.authCfg.LoginPassword {
		h.writeJSON(w, http.StatusUnauthorized, model.PublishResponse{
			Status:  "error",
			Message: "Invalid credentials",
		})
		return
	}

	// Generate JWT token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": req.Username,
		"exp": time.Now().Add(24 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.authCfg.JWTSecret))
	if err != nil {
		log.Error().Err(err).Msg("Failed to sign token")
		h.writeJSON(w, http.StatusInternalServerError, model.PublishResponse{
			Status:  "error",
			Message: "Failed to generate token",
		})
		return
	}

	h.writeJSON(w, http.StatusOK, model.LoginResponse{
		Token: tokenString,
	})
}

// JWTMiddleware validates the Bearer token in Authorization header.
func (h *Handler) JWTMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			h.writeJSON(w, http.StatusUnauthorized, model.PublishResponse{
				Status:  "error",
				Message: "Authorization header is required",
			})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			h.writeJSON(w, http.StatusUnauthorized, model.PublishResponse{
				Status:  "error",
				Message: "Authorization header format must be Bearer {token}",
			})
			return
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(h.authCfg.JWTSecret), nil
		})

		if err != nil || !token.Valid {
			h.writeJSON(w, http.StatusUnauthorized, model.PublishResponse{
				Status:  "error",
				Message: "Invalid or expired token",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}
