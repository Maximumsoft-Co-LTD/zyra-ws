package hub

import (
	"encoding/json"
	"log/slog"
	"sync"
	"unicode/utf8"
)

const maxChatLength = 500

// Room represents all connected clients within a single workspace.
type Room struct {
	workspaceID string
	clients     sync.Map // userID → *Client
	hub         *Hub
}

// register adds a client to the room and broadcasts a joined event to others.
// It also sends a welcome message directly to the new client with the full room state.
func (r *Room) register(c *Client) {
	// Build current player list before adding the new client.
	others := r.players(c.UserID)

	// Store the new client.
	r.clients.Store(c.UserID, c)

	slog.Info("ws room join", "workspace_id", r.workspaceID, "user_id", c.UserID, "online", r.count())

	// Send welcome to the new client.
	if msg, err := encode(MsgWelcome, WelcomePayload{Me: c.Player(), Players: others}); err == nil {
		c.Send(msg)
	}

	// Broadcast joined event to all others.
	if msg, err := encode(MsgJoined, JoinedPayload{Player: c.Player()}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

// unregister removes a client and broadcasts a left event.
func (r *Room) unregister(c *Client) {
	if _, loaded := r.clients.LoadAndDelete(c.UserID); !loaded {
		return
	}

	slog.Info("ws room leave", "workspace_id", r.workspaceID, "user_id", c.UserID, "online", r.count())

	if msg, err := encode(MsgLeft, LeftPayload{UserID: c.UserID}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}

	// Remove empty rooms from the hub.
	if r.count() == 0 {
		r.hub.removeRoom(r.workspaceID)
	}
}

// handleClientMessage routes an inbound JSON message from a client.
func (r *Room) handleClientMessage(c *Client, raw []byte) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		r.sendError(c, "invalid message format")
		return
	}

	switch env.Type {
	case ClientMsgMove:
		r.handleMove(c, env.Payload)
	case ClientMsgChat:
		r.handleChat(c, env.Payload)
	case ClientMsgPing:
		if msg, err := encode(MsgPong, nil); err == nil {
			c.Send(msg)
		}
	default:
		r.sendError(c, "unknown message type: "+env.Type)
	}
}

func (r *Room) handleMove(c *Client, payload json.RawMessage) {
	var p ClientMovePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid move payload")
		return
	}

	// Update in-place — no need to re-store because the pointer is shared.
	c.TileX = p.TileX
	c.TileY = p.TileY
	if p.AvatarURL != "" {
		c.AvatarURL = p.AvatarURL
	}

	// Broadcast movement to everyone else in the room.
	if msg, err := encode(MsgMoved, MovedPayload{
		UserID:    c.UserID,
		TileX:     c.TileX,
		TileY:     c.TileY,
		AvatarURL: c.AvatarURL,
	}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

func (r *Room) handleChat(c *Client, payload json.RawMessage) {
	var p ClientChatPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat payload")
		return
	}

	if p.Content == "" {
		return
	}

	// Truncate to prevent oversized payloads.
	if utf8.RuneCountInString(p.Content) > maxChatLength {
		runes := []rune(p.Content)
		p.Content = string(runes[:maxChatLength])
	}

	if msg, err := encode(MsgChat, ChatPayload{
		UserID:      c.UserID,
		DisplayName: c.DisplayName,
		Content:     p.Content,
	}); err == nil {
		r.broadcast(msg) // including sender so they see their own message
	}
}

// broadcast sends a message to every client in the room.
func (r *Room) broadcast(msg []byte) {
	r.clients.Range(func(_, v any) bool {
		v.(*Client).Send(msg)
		return true
	})
}

// broadcastExcept sends a message to all clients except excludeUserID.
func (r *Room) broadcastExcept(msg []byte, excludeUserID string) {
	r.clients.Range(func(k, v any) bool {
		if k.(string) != excludeUserID {
			v.(*Client).Send(msg)
		}
		return true
	})
}

// players returns a snapshot of all players in the room, excluding excludeUserID.
func (r *Room) players(excludeUserID string) []Player {
	var list []Player
	r.clients.Range(func(_, v any) bool {
		c := v.(*Client)
		if c.UserID != excludeUserID {
			list = append(list, c.Player())
		}
		return true
	})
	return list
}

// count returns the number of connected clients.
func (r *Room) count() int {
	n := 0
	r.clients.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (r *Room) sendError(c *Client, message string) {
	if msg, err := encode(MsgError, ErrorPayload{Message: message}); err == nil {
		c.Send(msg)
	}
}
