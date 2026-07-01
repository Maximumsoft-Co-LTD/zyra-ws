package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"zyra-ws/internal/config"
	"zyra-ws/internal/handler"
	"zyra-ws/internal/hub"
	"zyra-ws/internal/store"
)

func main() {
	cfg := config.Load()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Redis — optional; gracefully degrades to in-memory fallback when absent.
	var redisStore *store.RedisStore
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			slog.Warn("invalid REDIS_URL — running without Redis", "error", err)
		} else {
			rdb := redis.NewClient(opt)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if pingErr := rdb.Ping(ctx).Err(); pingErr != nil {
				slog.Warn("Redis ping failed — running without Redis", "error", pingErr)
			} else {
				redisStore = store.New(rdb)
				slog.Info("Redis connected", "url", cfg.RedisURL)
			}
		}
	} else {
		slog.Info("REDIS_URL not set — using in-memory fallback for presence/cooldowns")
	}

	h := hub.New(redisStore, cfg.DefaultCapacity)
	hnd := handler.New(h, cfg.TokenKey, cfg.AllowedOrigins)

	// Fan zyra-api's in-app notifications (Redis vo:notify) out to live connections
	// as chat:notification:new (SC-CHAT-10). No-op when Redis is absent.
	if redisStore != nil {
		notifCh, closeNotifSub := redisStore.SubscribeNotifications(context.Background())
		defer closeNotifSub() //nolint:errcheck
		go func() {
			for p := range notifCh {
				h.PushNotification(p.WorkspaceID, p.UserID, p.Notification)
			}
		}()
		slog.Info("notification push subscriber started", "channel", "vo:notify")
	}

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
		slog.Info("shutting down zyra-ws — draining clients")

		// 1. Notify all connected clients so they start reconnecting before
		//    their connection is torn down. nginx will route them to a live instance.
		h.Drain()

		// 2. Give clients enough time to receive the message and initiate a reconnect.
		//    Must be shorter than the rolling-deploy replacement window so the old
		//    instance is gone before the drain window expires.
		time.Sleep(3 * time.Second)

		// 3. Stop accepting new /ws connections and wait for in-flight handlers.
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
