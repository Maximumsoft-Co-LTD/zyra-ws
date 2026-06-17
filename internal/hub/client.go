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

	UserID      string
	DisplayName string
	AvatarURL   string
	TileX       int
	TileY       int
}

// Player converts the client's current state into a Player DTO.
func (c *Client) Player() Player {
	return Player{
		UserID:      c.UserID,
		DisplayName: c.DisplayName,
		AvatarURL:   c.AvatarURL,
		TileX:       c.TileX,
		TileY:       c.TileY,
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
