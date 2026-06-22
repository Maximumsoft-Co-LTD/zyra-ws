// Package hub manages workspace rooms and WebSocket client lifecycles.
package hub

import (
	"context"
	"log/slog"
	"sync"

	"github.com/gorilla/websocket"

	"zyra-ws/internal/store"
)

// Hub owns all active workspace rooms.
// It is goroutine-safe.
type Hub struct {
	rooms           sync.Map // workspaceID → *Room
	store           *store.RedisStore
	defaultCapacity int
}

// New creates a Hub. A single Hub should be shared across all handlers.
func New(redisStore *store.RedisStore, defaultCapacity int) *Hub {
	if defaultCapacity <= 0 {
		defaultCapacity = 50
	}
	return &Hub{store: redisStore, defaultCapacity: defaultCapacity}
}

// Join registers a new WebSocket connection into the workspace room.
// If the room is at capacity, it sends capacity_reached and closes the connection.
// tileX/tileY are the client's initial spawn tile (0 = unknown/use server default).
// Control returns immediately; the goroutines own the connection from here on.
func (h *Hub) Join(
	conn *websocket.Conn,
	userID, displayName, characterName, avatarURL, workspaceID string,
	capacity int, // from API; 0 = use default
	tileX, tileY int, // initial spawn tile passed by the client on connect
) {
	cap := capacity
	if cap <= 0 {
		cap = h.defaultCapacity
	}

	room := h.getOrCreateRoom(workspaceID)

	// Capacity check: count in-memory clients + active Redis presence keys.
	online := room.count()
	if h.store != nil {
		if redisCount, err := h.store.OnlineCount(context.Background(), workspaceID); err == nil && redisCount > online {
			online = redisCount
		}
	}

	if online >= cap {
		slog.Warn("ws capacity reached", "workspace_id", workspaceID, "online", online, "capacity", cap)
		if msg, err := encode(MsgCapacityReached, CapacityReachedPayload{Message: "office is full"}); err == nil {
			conn.WriteMessage(1, msg) //nolint:errcheck // 1 = TextMessage
		}
		conn.Close()
		return
	}

	c := &Client{
		hub:           h,
		room:          room,
		conn:          conn,
		send:          make(chan []byte, 64),
		UserID:        userID,
		DisplayName:   displayName,
		CharacterName: characterName,
		AvatarURL:     avatarURL,
		TileX:         tileX,
		TileY:         tileY,
	}

	// Restore last known tile from Redis (written by unregister on disconnect).
	// This overrides the client-provided tile so that the joined broadcast and
	// the welcome.me payload both carry the correct pre-disconnect position,
	// preventing other clients from seeing a teleport-to-spawn on reload.
	if h.store != nil {
		ctx := context.Background()
		if lx, ly, err := h.store.GetLastPosition(ctx, workspaceID, userID); err == nil && (lx != 0 || ly != 0) {
			c.TileX = lx
			c.TileY = ly
			slog.Info("ws restore last position", "user_id", userID, "workspace_id", workspaceID, "tile_x", lx, "tile_y", ly)
		}
	}

	room.register(c)

	// Persist presence to Redis with the effective spawn tile (last-position or client-provided).
	if h.store != nil {
		ctx := context.Background()
		_ = h.store.SetPresence(ctx, workspaceID, userID, displayName, avatarURL, "available", c.TileX, c.TileY)
	}

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
