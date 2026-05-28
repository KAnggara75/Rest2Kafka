package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/KAnggara75/Rest2Kafka/internal/config"
	"github.com/KAnggara75/Rest2Kafka/internal/handler"
	"github.com/KAnggara75/Rest2Kafka/internal/kafka"
	"github.com/KAnggara75/Rest2Kafka/internal/service"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Configure zerolog level from ENV
	logLevel := strings.ToLower(os.Getenv("LOG_LEVEL"))
	level := zerolog.DebugLevel
	switch logLevel {
	case "info":
		level = zerolog.InfoLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	}
	zerolog.SetGlobalLevel(level)

	// Configure pretty logging for console output
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(os.Stdout).With().
		Str("service", "Rest2Kafka").
		Logger()

	log.Info().Msg("Starting Kafka Publish Service...")

	// 1. Load config
	cfgPath := ".env"
	if envPath := os.Getenv("ENV_PATH"); envPath != "" {
		cfgPath = envPath
	}

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// 2. Initialize Kafka Manager and Publish Service
	kafkaManager := kafka.NewManager(cfg)
	defer func() {
		log.Info().Msg("Closing Kafka connections...")
		if err := kafkaManager.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing Kafka manager")
		}
	}()

	pubService := service.NewPublishService(kafkaManager)

	// 3. Setup HTTP Handler and Routing
	h := handler.NewHandler(pubService)
	mux := h.RegisterRoutes()

	// 4. Configure HTTP Server
	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	server := &http.Server{
		Addr:         serverAddr,
		Handler:      handler.LoggingMiddleware(mux),
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSeconds) * time.Second,
	}

	// 5. Graceful shutdown setup
	serverErrors := make(chan error, 1)
	go func() {
		log.Info().Msgf("HTTP Server listening on %s", serverAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// 6. Block until signal or server error
	select {
	case err := <-serverErrors:
		log.Fatal().Err(err).Msg("HTTP server error")

	case sig := <-shutdown:
		log.Info().Msgf("Received shutdown signal: %v. Initiating graceful shutdown...", sig)

		// Create context with timeout for server shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("HTTP server graceful shutdown failed")
			if err := server.Close(); err != nil {
				log.Error().Err(err).Msg("HTTP server hard close failed")
			}
		}
	}

	log.Info().Msg("Service stopped.")
}
