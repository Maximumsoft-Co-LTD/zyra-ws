package handler

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"

	"zyra-ws/internal/auth"
	"zyra-ws/internal/hub"
)

// Handler exposes HTTP endpoints for zyra-ws.
type Handler struct {
	hub      *hub.Hub
	tokenKey string
	upgrader websocket.Upgrader
}

// New creates a Handler.
// allowedOrigins is the list from config; "*" disables origin check.
func New(h *hub.Hub, tokenKey string, allowedOrigins []string) *Handler {
	wildcardAll := len(allowedOrigins) == 1 && allowedOrigins[0] == "*"

	upgrader := websocket.Upgrader{
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
		CheckOrigin: func(r *http.Request) bool {
			if wildcardAll {
				return true
			}
			origin := r.Header.Get("Origin")
			for _, allowed := range allowedOrigins {
				if strings.EqualFold(origin, allowed) {
					return true
				}
			}
			slog.Warn("ws origin rejected", "origin", origin, "allowed", allowedOrigins)
			return false
		},
	}

	return &Handler{hub: h, tokenKey: tokenKey, upgrader: upgrader}
}

// Healthz responds with service status and per-room online counts.
// GET /healthz
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	stats := h.hub.Stats()
	total := 0
	for _, n := range stats {
		total += n
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Hand-rolled JSON to avoid importing encoding/json for a trivial response.
	rooms := ""
	for id, n := range stats {
		if rooms != "" {
			rooms += ","
		}
		rooms += `"` + id + `":` + itoa(n)
	}
	w.Write([]byte(`{"status":"ok","total_online":` + itoa(total) + `,"rooms":{` + rooms + `}}`)) //nolint:errcheck
}

// Connect upgrades an HTTP request to a WebSocket connection and joins the hub.
// GET /ws?workspace_id=<id>&token=<jwt>[&avatar_url=<url>][&character_name=<name>][&capacity=<n>][&tile_x=<n>&tile_y=<n>][&client_session_id=<id>]
func (h *Handler) Connect(w http.ResponseWriter, r *http.Request) {
	workspaceID := strings.TrimSpace(r.URL.Query().Get("workspace_id"))
	tokenStr := strings.TrimSpace(r.URL.Query().Get("token"))
	avatarURL := strings.TrimSpace(r.URL.Query().Get("avatar_url"))
	characterName := strings.TrimSpace(r.URL.Query().Get("character_name"))
	capacityStr := strings.TrimSpace(r.URL.Query().Get("capacity"))
	tileXStr := strings.TrimSpace(r.URL.Query().Get("tile_x"))
	tileYStr := strings.TrimSpace(r.URL.Query().Get("tile_y"))
	clientSessionID := strings.TrimSpace(r.URL.Query().Get("client_session_id"))

	if workspaceID == "" {
		http.Error(w, "workspace_id required", http.StatusBadRequest)
		return
	}

	claims, err := auth.ValidateToken(tokenStr, h.tokenKey)
	if err != nil {
		slog.Warn("ws auth rejected", "error", err, "workspace_id", workspaceID)
		http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	capacity, _ := strconv.Atoi(capacityStr) // 0 = use hub default
	tileX, _ := strconv.Atoi(tileXStr)       // 0 = server will use spawn default
	tileY, _ := strconv.Atoi(tileYStr)

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote the error response; nothing to do.
		return
	}

	h.hub.Join(conn, claims.UserID, claims.DisplayName, characterName, avatarURL, workspaceID, clientSessionID, capacity, tileX, tileY)
}

// itoa is a tiny int-to-string helper to avoid importing strconv here.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 10)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
