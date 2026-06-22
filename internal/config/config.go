package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port string

	// Must match zyra-api tokenKey to validate access tokens.
	TokenKey string

	// Comma-separated list of allowed WebSocket origins.
	// "*" accepts all (development only).
	AllowedOrigins []string

	// Max message size in bytes from clients.
	MaxMessageBytes int64

	// Redis URL for presence, cooldowns, follow state.
	// Empty = Redis disabled (in-memory fallback).
	RedisURL string

	// Default workspace capacity when not specified in the DB.
	DefaultCapacity int
}

func Load() Config {
	_ = godotenv.Load()

	originsRaw := getenv("ALLOWED_ORIGINS", "*")
	var origins []string
	for _, o := range strings.Split(originsRaw, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins = append(origins, o)
		}
	}

	capacity := 50
	if v := os.Getenv("DEFAULT_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			capacity = n
		}
	}

	return Config{
		Port:            getenv("PORT", "3004"),
		TokenKey:        getenv("tokenKey", "change-me"),
		AllowedOrigins:  origins,
		MaxMessageBytes: 4096,
		RedisURL:        getenv("REDIS_URL", ""),
		DefaultCapacity: capacity,
	}
}

func getenv(name, defaultValue string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultValue
}
