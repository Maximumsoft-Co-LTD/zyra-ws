package hub

import (
	"log/slog"
	"sync"
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
	hub       *Hub
	room      *Room
	conn      *websocket.Conn
	send      chan []byte // buffered outbound JSON (TextMessage)
	sendBin   chan []byte // buffered outbound binary frames (BinaryMessage)
	closeOnce sync.Once   // ensures send channel is closed at most once

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
	// Follow chain (doubly-linked list). The chain is a single line:
	//   Leader <- F1 <- F2 <- F3(tail)   (new followers append at the tail)
	// FollowTargetID = the node directly AHEAD (the one this client walks behind).
	// FollowerID     = the node directly BEHIND (at most one — the chain is linear).
	// Tracked in-memory so detach/heal can re-link neighbours without a Redis round-trip.
	FollowTargetID string
	FollowerID     string

	// Hidden is true while the client's tab is backgrounded (Page Visibility). A
	// hidden follower can't drive its own movement (its rAF/timers are paused), so
	// the server takes over walking it behind its predecessor — this keeps a chain
	// alive when a middle node is backgrounded (otherwise everyone behind it freezes).
	Hidden bool

	// IsMoving is true while the player is walking a path (between move_to and stop).
	// MoveStartTX/TY stores the tile when the walk began so that unregister
	// can persist the pre-move position on abrupt disconnect instead of the
	// unreached destination tile.
	IsMoving    bool
	MoveStartTX int
	MoveStartTY int

	// Active path movement state — retained so newly joined clients can pick up
	// the in-progress walk and start interpolating from the correct point.
	MovePath       []TilePoint
	MoveDurationMs int
	MoveSpeed      float64
	MoveStartedAt  time.Time

	// lastSnapAt is the wall-clock time of the last SavePosSnapshot Redis write.
	// Snapshots are throttled to at most 1 per second — new joiners only need
	// periodic accuracy, not a write on every 20 Hz move.
	lastSnapAt time.Time

	// chatConvs is the reverse index of the conversations this client subscribed
	// to (chat:join). It lets unregister remove the client from every conversation
	// in Room.chatSubs without scanning the whole registry, preventing phantom
	// relays after disconnect. Accessed only under Room.chatMu (CHAT-006).
	chatConvs map[string]struct{}
}

// Player converts the client's current state into a Player DTO.
func (c *Client) Player() Player {
	p := Player{
		UserID:         c.UserID,
		DisplayName:    c.DisplayName,
		CharacterName:  c.CharacterName,
		AvatarURL:      c.AvatarURL,
		TileX:          c.TileX,
		TileY:          c.TileY,
		PX:             c.PX,
		PY:             c.PY,
		Status:         c.Status,
		CustomMsg:      c.CustomMsg,
		RoomID:         c.RoomID,
		Direction:      c.Direction,
		Sitting:        c.Sitting,
		FollowTargetID: c.FollowTargetID,
	}
	if c.IsMoving && len(c.MovePath) >= 2 {
		elapsed := int(time.Since(c.MoveStartedAt).Milliseconds())
		if elapsed < c.MoveDurationMs {
			p.ActivePath = c.MovePath
			p.ActiveDurationMs = c.MoveDurationMs
			p.ActiveSpeed = c.MoveSpeed
			p.ActiveElapsedMs = elapsed
		}
	}
	return p
}

// WritePump pumps messages from the send channels to the WebSocket connection.
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

		case msg := <-c.sendBin:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
			if err := c.conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
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

// Send enqueues a JSON message for the client's write pump.
// If the send buffer is full the client is unregistered and the send channel
// is closed exactly once (via closeOnce) to signal WritePump to exit.
func (c *Client) Send(msg []byte) {
	select {
	case c.send <- msg:
	default:
		slog.Warn("ws send buffer full — dropping client", "user_id", c.UserID)
		c.room.unregister(c)
		c.closeOnce.Do(func() { close(c.send) })
	}
}

// SendBin enqueues a binary frame for the client's write pump.
//
// Binary frames are idempotent, latest-wins position snapshots: each 20 ms tick
// carries a player's newest position, so a lost frame is harmless — the next tick
// supersedes it. Under a transient burst (a slow or briefly backgrounded peer at
// 20 Hz × many movers) the buffer can fill momentarily. Dropping the WHOLE client
// here — as the old code did — disconnects the user over a hiccup, forcing them to
// refresh to recover the stream: the high-concurrency desync users reported.
//
// Instead, drop one stale frame and enqueue the newest. A genuinely dead client is
// still reaped by the WritePump write deadline / ping-pong, so connections don't
// leak. SendBin is only ever called from the single per-room move-ticker goroutine,
// so this drain-and-replace is race-free for a given client.
func (c *Client) SendBin(msg []byte) {
	select {
	case c.sendBin <- msg:
	default:
		select {
		case <-c.sendBin: // discard the oldest (now-stale) position
		default:
		}
		select {
		case c.sendBin <- msg:
		default:
		}
	}
}
