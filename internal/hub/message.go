package hub

import "encoding/json"

// ── Outbound message types (server → client) ──────────────────────────────────

const (
	MsgWelcome = "welcome" // sent once to the joining client: full room state
	MsgJoined  = "joined"  // broadcast: another player entered the room
	MsgLeft    = "left"    // broadcast: a player disconnected
	MsgMoved   = "moved"   // broadcast: a player changed tile position
	MsgChat    = "chat"    // broadcast: a player sent a chat message
	MsgPong    = "pong"    // response to client ping
	MsgError   = "error"   // server-side error feedback
)

// Envelope is the top-level wrapper for all messages in both directions.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Player is the shared player representation used in payloads.
type Player struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
	TileX       int    `json:"tile_x"`
	TileY       int    `json:"tile_y"`
}

// ── Outbound payloads ─────────────────────────────────────────────────────────

type WelcomePayload struct {
	Me      Player   `json:"me"`
	Players []Player `json:"players"` // others currently in the room
}

type JoinedPayload struct {
	Player Player `json:"player"`
}

type LeftPayload struct {
	UserID string `json:"user_id"`
}

type MovedPayload struct {
	UserID    string `json:"user_id"`
	TileX     int    `json:"tile_x"`
	TileY     int    `json:"tile_y"`
	AvatarURL string `json:"avatar_url"`
}

type ChatPayload struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Content     string `json:"content"`
}

type ErrorPayload struct {
	Message string `json:"message"`
}

// ── Inbound message types (client → server) ───────────────────────────────────

const (
	ClientMsgMove = "move"
	ClientMsgChat = "chat"
	ClientMsgPing = "ping"
)

type ClientMovePayload struct {
	TileX     int    `json:"tile_x"`
	TileY     int    `json:"tile_y"`
	AvatarURL string `json:"avatar_url"`
}

type ClientChatPayload struct {
	Content string `json:"content"`
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// encode serialises an Envelope into JSON bytes.
func encode(msgType string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Type: msgType, Payload: raw})
}
