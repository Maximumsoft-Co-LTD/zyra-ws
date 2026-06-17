// Package hub manages workspace rooms and WebSocket client lifecycles.
package hub

import (
	"log/slog"
	"sync"

	"github.com/gorilla/websocket"
)

// Hub owns all active workspace rooms.
// It is goroutine-safe.
type Hub struct {
	rooms sync.Map // workspaceID → *Room
}

// New creates a Hub. A single Hub should be shared across all handlers.
func New() *Hub {
	return &Hub{}
}

// Join registers a new WebSocket connection into the workspace room,
// then starts the client's read/write goroutines.
// Control returns immediately; the goroutines own the connection from here on.
func (h *Hub) Join(
	conn *websocket.Conn,
	userID, displayName, avatarURL, workspaceID string,
) {
	room := h.getOrCreateRoom(workspaceID)

	c := &Client{
		hub:         h,
		room:        room,
		conn:        conn,
		send:        make(chan []byte, 64),
		UserID:      userID,
		DisplayName: displayName,
		AvatarURL:   avatarURL,
	}

	room.register(c)

	go c.WritePump()
	go c.ReadPump()
}

// Stats returns workspaceID → online count (used by /healthz).
func (h *Hub) Stats() map[string]int {
	stats := make(map[string]int)
	h.rooms.Range(func(k, v any) bool {
		stats[k.(string)] = v.(*Room).count()
		return true
	})
	return stats
}

func (h *Hub) getOrCreateRoom(workspaceID string) *Room {
	if v, ok := h.rooms.Load(workspaceID); ok {
		return v.(*Room)
	}
	room := &Room{workspaceID: workspaceID, hub: h}
	actual, _ := h.rooms.LoadOrStore(workspaceID, room)
	return actual.(*Room)
}

func (h *Hub) removeRoom(workspaceID string) {
	h.rooms.Delete(workspaceID)
	slog.Info("ws room removed (empty)", "workspace_id", workspaceID)
}
