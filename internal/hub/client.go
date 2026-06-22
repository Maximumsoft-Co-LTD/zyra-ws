package hub

import (
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the client.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message.
	pongWait = 60 * time.Second

	// Send pings to client with this period.  Must be < pongWait.
	pingPeriod = 45 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 4096
)

// Client represents a single WebSocket connection inside a Room.
type Client struct {
	hub  *Hub
	room *Room
	conn *websocket.Conn
	send chan []byte // buffered outbound messages

	UserID        string
	DisplayName   string
	CharacterName string
	AvatarURL     string
	TileX         int
	TileY         int
	PX            float64 // world pixel X (sprite centre) — 0 = not yet set
	PY            float64 // world pixel Y (sprite centre) — 0 = not yet set
	Status        string
	CustomMsg     string
	RoomID        string
	Direction     string
	Sitting       bool
	// FollowTargetID is the user_id this client is currently following.
	// Tracked in-memory so the unfollow handler can notify the previous target
	// without an extra Redis round-trip.
	FollowTargetID string
}

// Player converts the client's current state into a Player DTO.
func (c *Client) Player() Player {
	return Player{
		UserID:        c.UserID,
		DisplayName:   c.DisplayName,
		CharacterName: c.CharacterName,
		AvatarURL:     c.AvatarURL,
		TileX:         c.TileX,
		TileY:         c.TileY,
		PX:            c.PX,
		PY:            c.PY,
		Status:        c.Status,
		CustomMsg:     c.CustomMsg,
		RoomID:        c.RoomID,
		Direction:     c.Direction,
		Sitting:       c.Sitting,
	}
}

// WritePump pumps messages from the send channel to the WebSocket connection.
// Each client has exactly one WritePump goroutine.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
			if !ok {
				// Channel closed — send close frame and return.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{}) //nolint:errcheck
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump pumps messages from the WebSocket connection into the hub.
// Each client has exactly one ReadPump goroutine.
// When ReadPump returns, the client is unregistered from its room.
func (c *Client) ReadPump() {
	defer func() {
		c.room.unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Warn("ws client read error", "user_id", c.UserID, "error", err)
			}
			break
		}
		c.room.handleClientMessage(c, raw)
	}
}

// EffectiveName returns the character name if set, otherwise falls back to display name.
func (c *Client) EffectiveName() string {
	if c.CharacterName != "" {
		return c.CharacterName
	}
	return c.DisplayName
}

// Send enqueues a message for the client's write pump.
// Drops the message and closes the connection if the send buffer is full.
func (c *Client) Send(msg []byte) {
	select {
	case c.send <- msg:
	default:
		slog.Warn("ws send buffer full — dropping client", "user_id", c.UserID)
		c.room.unregister(c)
		close(c.send)
	}
}
