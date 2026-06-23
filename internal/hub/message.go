package hub

import (
	"encoding/binary"
	"encoding/json"
	"math"
)

// ── Outbound message types (server → client) ──────────────────────────────────

const (
	MsgWelcome = "welcome" // sent once to the joining client: full room state
	MsgJoined  = "joined"  // broadcast: another player entered the room
	MsgLeft    = "left"    // broadcast: a player disconnected
	MsgMoved   = "moved"   // broadcast: a player changed tile position
	MsgChat    = "chat"    // broadcast: a player sent a chat message
	MsgPong    = "pong"    // response to client ping
	MsgError   = "error"   // server-side error feedback

	MsgStatusChanged = "status_changed" // broadcast: a player changed their status
	MsgRoomEntered   = "room_entered"   // broadcast: a player entered a private room
	MsgRoomExited    = "room_exited"    // broadcast: a player exited a private room
	MsgWaveReceived  = "wave_received"  // unicast: target player received a wave
	MsgFollowStarted  = "follow_started"  // unicast: target player is being followed
	MsgFollowEnded    = "follow_ended"    // unicast: follower stopped following (sent to target)
	MsgFollowRevoked  = "follow_revoked"  // unicast: target stopped the follow (sent to follower)
	MsgKnockRequest  = "knock_request"  // broadcast: someone knocked on a zone
	MsgKnockGranted    = "knock_granted"    // unicast: knock was granted
	MsgKnockDenied     = "knock_denied"     // unicast: knock was denied
	MsgKnockDecided    = "knock_decided"    // broadcast to room: a decision was made (dismiss notification on all occupants)
	MsgKnockCancelled  = "knock_cancelled"  // broadcast to room: requester cancelled their knock (dismiss notification on all occupants)
	MsgCapacityReached = "capacity_reached" // unicast: office is full — connection will be closed
	MsgSectionSync     = "section_sync"     // broadcast: section state changed (relay from any client)
	MsgWaveAnimation   = "wave_animation"   // broadcast: show wave animation on sender's avatar
	MsgServerDrain     = "server_drain"     // broadcast: server is shutting down — client should reconnect
)

// Envelope is the top-level wrapper for all messages in both directions.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Player is the shared player representation used in payloads.
type Player struct {
	UserID        string  `json:"user_id"`
	DisplayName   string  `json:"display_name"`
	CharacterName string  `json:"character_name,omitempty"`
	AvatarURL     string  `json:"avatar_url"`
	TileX         int     `json:"tile_x"`
	TileY         int     `json:"tile_y"`
	PX            float64 `json:"px,omitempty"` // world pixel X (sprite centre)
	PY            float64 `json:"py,omitempty"` // world pixel Y (sprite centre)
	Status        string  `json:"status,omitempty"`
	CustomMsg     string  `json:"custom_msg,omitempty"`
	RoomID        string  `json:"room_id,omitempty"`
	Direction     string  `json:"direction,omitempty"`
	Sitting       bool    `json:"sitting,omitempty"`
}

// ── Outbound payloads ─────────────────────────────────────────────────────────

// PendingKnock is included in the welcome message so the REQUESTER can restore
// their "waiting for access" state after a page reload.
type PendingKnock struct {
	RequestID string `json:"request_id"`
	ZoneID    string `json:"zone_id"`
}

// ActiveKnockRequest is included in the welcome message so OCCUPANTS can restore
// pending knock notifications they had received before a page reload.
type ActiveKnockRequest struct {
	RequestID       string `json:"request_id"`
	ZoneID          string `json:"zone_id"`
	RequesterUserID string `json:"requester_user_id"`
	RequesterName   string `json:"requester_name"`
	RequesterAvatar string `json:"requester_avatar"`
}

type WelcomePayload struct {
	Me                  Player               `json:"me"`
	Players             []Player             `json:"players"`              // others currently in the room
	PendingKnocks       []PendingKnock       `json:"pending_knocks,omitempty"`       // requester: own outgoing knocks
	ActiveKnockRequests []ActiveKnockRequest `json:"active_knock_requests,omitempty"` // occupant: incoming knocks for this workspace
}

type JoinedPayload struct {
	Player Player `json:"player"`
}

type LeftPayload struct {
	UserID string `json:"user_id"`
}

type MovedPayload struct {
	UserID    string  `json:"user_id"`
	TileX     int     `json:"tile_x"`
	TileY     int     `json:"tile_y"`
	PX        float64 `json:"px,omitempty"` // world pixel X (sprite centre)
	PY        float64 `json:"py,omitempty"` // world pixel Y (sprite centre)
	AvatarURL string  `json:"avatar_url"`
	Direction string  `json:"direction,omitempty"`
	// Sitting must NOT use omitempty — false must be broadcast explicitly so receivers
	// can clear the sitting state when a player stands up.
	Sitting bool `json:"sitting"`
}

type ChatPayload struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Content     string `json:"content"`
}

type ErrorPayload struct {
	Message string `json:"message"`
}

type StatusChangedPayload struct {
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	CustomMsg string `json:"custom_msg,omitempty"`
}

type RoomChangedPayload struct {
	UserID string `json:"user_id"`
	RoomID string `json:"room_id"` // empty = exited
}

type WaveReceivedPayload struct {
	SenderUserID    string `json:"sender_user_id"`
	SenderName      string `json:"sender_name"`
	SenderAvatarURL string `json:"sender_avatar_url"`
}

type WaveAnimationPayload struct {
	UserID string `json:"user_id"` // the player who is waving (show animation on their avatar)
}

type KnockRequestPayload struct {
	RequestID       string `json:"request_id"`
	ZoneID          string `json:"zone_id"`
	RequesterUserID string `json:"requester_user_id"`
	RequesterName   string `json:"requester_name"`
	RequesterAvatar string `json:"requester_avatar"`
}

type KnockResultPayload struct {
	RequestID string `json:"request_id"`
	ZoneID    string `json:"zone_id"`
	Granted   bool   `json:"granted"`
}

type FollowPayload struct {
	FollowerUserID string `json:"follower_user_id"`
	FollowerName   string `json:"follower_name"`
	FollowerAvatar string `json:"follower_avatar"`
	Following      bool   `json:"following"` // true=started, false=ended
}

type CapacityReachedPayload struct {
	Message string `json:"message"`
}

// ServerDrainPayload is broadcast to all clients when the server is about to shut down.
// Clients should begin reconnecting immediately; nginx will route them to a live instance.
type ServerDrainPayload struct {
	Reason            string `json:"reason"`             // always "server_shutdown"
	ReconnectAfterMs  int    `json:"reconnect_after_ms"` // suggested reconnect delay in ms
}

// KnockDecidedPayload is broadcast to all room occupants so they can dismiss the notification.
type KnockDecidedPayload struct {
	RequestID string `json:"request_id"`
	ZoneID    string `json:"zone_id"`
}

// KnockDeniedPayload includes cooldown info so the client can show the countdown.
type KnockDeniedPayload struct {
	RequestID    string `json:"request_id"`
	ZoneID       string `json:"zone_id"`
	CooldownSec  int    `json:"cooldown_sec"`
	DenierUserID string `json:"denier_user_id"`
	DenierName   string `json:"denier_name"`
	DenierAvatar string `json:"denier_avatar"`
}

// ── Inbound message types (client → server) ───────────────────────────────────

const (
	ClientMsgMove        = "move"
	ClientMsgChat        = "chat"
	ClientMsgPing        = "ping"
	ClientMsgStatus      = "status"
	ClientMsgRoomEnter   = "room_enter"
	ClientMsgRoomExit    = "room_exit"
	ClientMsgWave        = "wave"
	ClientMsgFollow       = "follow"
	ClientMsgStopFollower = "stop_follower" // followee → server: kick a specific follower
	ClientMsgKnock        = "knock"
	ClientMsgKnockDecide = "knock_decision"
	ClientMsgKnockCancel = "knock_cancel"
	ClientMsgHeartbeat   = "heartbeat"
	ClientMsgSectionSync = "section_sync" // client→server→broadcast: notify peers of section state change
)

type ClientMovePayload struct {
	TileX     int     `json:"tile_x"`
	TileY     int     `json:"tile_y"`
	PX        float64 `json:"px,omitempty"` // world pixel X (sprite centre)
	PY        float64 `json:"py,omitempty"` // world pixel Y (sprite centre)
	AvatarURL string  `json:"avatar_url"`
	Direction string  `json:"direction,omitempty"`
	Sitting   bool    `json:"sitting,omitempty"`
}

type ClientChatPayload struct {
	Content string `json:"content"`
}

type ClientStatusPayload struct {
	Status    string `json:"status"`
	CustomMsg string `json:"custom_msg"`
}

type ClientRoomPayload struct {
	RoomID string `json:"room_id"`
}

type ClientWavePayload struct {
	TargetUserID string `json:"target_user_id"`
}

type ClientFollowPayload struct {
	TargetUserID string `json:"target_user_id"` // empty = stop following
}

type ClientStopFollowerPayload struct {
	FollowerUserID string `json:"follower_user_id"`
}

type ClientKnockPayload struct {
	ZoneID string `json:"zone_id"`
}

type ClientKnockDecidePayload struct {
	RequestID string `json:"request_id"`
	ZoneID    string `json:"zone_id"`
	Allow     bool   `json:"allow"`
}

// ClientKnockCancelPayload is sent by the requester to cancel their pending knock.
// The server identifies the pending knock by requester user ID + zone ID.
type ClientKnockCancelPayload struct {
	ZoneID string `json:"zone_id"`
}

// KnockCancelledPayload is broadcast to all room occupants so they can dismiss the notification.
type KnockCancelledPayload struct {
	RequestID string `json:"request_id"`
	ZoneID    string `json:"zone_id"`
}

// SectionSyncPayload is relayed verbatim to all clients when any player enters/leaves/grants a zone section.
type SectionSyncPayload struct {
	ZoneID      string   `json:"zone_id"`
	SectionID   string   `json:"section_id,omitempty"`
	MemberCount int      `json:"member_count"`
	IsLocked    bool     `json:"is_locked"`
	MemberIDs   []string `json:"member_ids"`
	Ended       bool     `json:"ended,omitempty"`
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

// ── Binary moved_bin frame ────────────────────────────────────────────────────
//
// Layout (little-endian, WebSocket BinaryMessage):
//
//	[0]         : 0x01 (BinTypeMoved)
//	[1]         : N — user_id length (uint8, max 255)
//	[2..N+1]    : user_id (UTF-8 bytes)
//	[N+2..N+3]  : tile_x (int16)
//	[N+4..N+5]  : tile_y (int16)
//	[N+6..N+9]  : px (float32) — world pixel X
//	[N+10..N+13]: py (float32) — world pixel Y
//	[N+14]      : direction byte (0=none 1=up 2=down 3=left 4=right)
//	[N+15]      : flags (bit 0 = sitting)
//
// avatar_url is intentionally omitted: receivers already hold it from the
// joined/welcome event and it changes rarely.

const BinTypeMoved = byte(0x01)

const (
	binDirNone  = uint8(0)
	binDirUp    = uint8(1)
	binDirDown  = uint8(2)
	binDirLeft  = uint8(3)
	binDirRight = uint8(4)
)

func encodeDirection(dir string) uint8 {
	switch dir {
	case "up":
		return binDirUp
	case "down":
		return binDirDown
	case "left":
		return binDirLeft
	case "right":
		return binDirRight
	default:
		return binDirNone
	}
}

// encodeBinMoved serialises a MovedPayload as a compact binary frame.
// It never fails — callers can use the result directly without error handling.
func encodeBinMoved(moved MovedPayload) []byte {
	id := []byte(moved.UserID)
	if len(id) > 255 {
		id = id[:255]
	}
	buf := make([]byte, 1+1+len(id)+2+2+4+4+1+1)
	i := 0
	buf[i] = BinTypeMoved
	i++
	buf[i] = uint8(len(id))
	i++
	copy(buf[i:], id)
	i += len(id)
	binary.LittleEndian.PutUint16(buf[i:], uint16(int16(moved.TileX)))
	i += 2
	binary.LittleEndian.PutUint16(buf[i:], uint16(int16(moved.TileY)))
	i += 2
	binary.LittleEndian.PutUint32(buf[i:], math.Float32bits(float32(moved.PX)))
	i += 4
	binary.LittleEndian.PutUint32(buf[i:], math.Float32bits(float32(moved.PY)))
	i += 4
	buf[i] = encodeDirection(moved.Direction)
	i++
	if moved.Sitting {
		buf[i] = 1
	}
	return buf
}

// decodeBinMoved parses a binary moved frame produced by encodeBinMoved.
// Returns nil if the frame is malformed or has the wrong type byte.
func decodeBinMoved(b []byte) *MovedPayload {
	if len(b) < 2 || b[0] != BinTypeMoved {
		return nil
	}
	idLen := int(b[1])
	if len(b) < 2+idLen+2+2+4+4+1+1 {
		return nil
	}
	i := 2
	userID := string(b[i : i+idLen])
	i += idLen
	tileX := int(int16(binary.LittleEndian.Uint16(b[i:])))
	i += 2
	tileY := int(int16(binary.LittleEndian.Uint16(b[i:])))
	i += 2
	px := float64(math.Float32frombits(binary.LittleEndian.Uint32(b[i:])))
	i += 4
	py := float64(math.Float32frombits(binary.LittleEndian.Uint32(b[i:])))
	i += 4
	var direction string
	switch b[i] {
	case binDirUp:
		direction = "up"
	case binDirDown:
		direction = "down"
	case binDirLeft:
		direction = "left"
	case binDirRight:
		direction = "right"
	}
	i++
	sitting := (b[i] & 0x01) != 0
	return &MovedPayload{
		UserID:    userID,
		TileX:     tileX,
		TileY:     tileY,
		PX:        px,
		PY:        py,
		Direction: direction,
		Sitting:   sitting,
	}
}
