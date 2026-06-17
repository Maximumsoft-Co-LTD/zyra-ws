package config

import (
	"os"
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

	return Config{
		Port:            getenv("PORT", "3004"),
		TokenKey:        getenv("tokenKey", "change-me"),
		AllowedOrigins:  origins,
		MaxMessageBytes: 4096,
	}
}

func getenv(name, defaultValue string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultValue
}
