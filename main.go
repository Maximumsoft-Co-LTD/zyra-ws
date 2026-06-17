package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"zyra-ws/internal/config"
	"zyra-ws/internal/handler"
	"zyra-ws/internal/hub"
)

func main() {
	cfg := config.Load()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	h := hub.New()
	hnd := handler.New(h, cfg.TokenKey, cfg.AllowedOrigins)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", hnd.Healthz)
	mux.HandleFunc("GET /ws", hnd.Connect)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // WebSocket connections are long-lived; no write timeout
		IdleTimeout:  120 * time.Second,
	}

	done := make(chan struct{})
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		slog.Info("shutting down zyra-ws")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx) //nolint:errcheck
		close(done)
	}()

	slog.Info("zyra-ws started", "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("listen failed", "error", err)
		os.Exit(1)
	}
	<-done
}
