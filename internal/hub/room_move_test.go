package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func findMoved(msgs []Envelope) (*MovedPayload, bool) {
	for _, m := range msgs {
		if m.Type != MsgMoved {
			continue
		}
		var p MovedPayload
		if err := json.Unmarshal(m.Payload, &p); err == nil {
			return &p, true
		}
	}
	return nil, false
}

// findMovedWithAvatarURL returns the first moved event that carries a non-empty AvatarURL.
// Used to locate the JSON broadcast that fires when a player's avatar_url changes,
// since the binary moved_bin frames intentionally omit that field.
func findMovedWithAvatarURL(msgs []Envelope) (*MovedPayload, bool) {
	for _, m := range msgs {
		if m.Type != MsgMoved {
			continue
		}
		var p MovedPayload
		if err := json.Unmarshal(m.Payload, &p); err == nil && p.AvatarURL != "" {
			return &p, true
		}
	}
	return nil, false
}

// ── handleMove — tick-buffered broadcast ──────────────────────────────────────
// handleMove buffers the latest position per player; flushMoves() sends to peers.
// Tests call r.flushMoves() explicitly to simulate a 50ms tick.

func TestHandleMove_BroadcastsOnTick(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{TileX: 5, TileY: 3, PX: 160, PY: 96, Direction: "right"})
	r.handleMove(mover, payload)
	r.flushMoves()

	p, ok := findMoved(drain(other))
	require.True(t, ok, "moved must be broadcast to peers after tick flush")
	assert.Equal(t, 5, p.TileX)
	assert.Equal(t, 3, p.TileY)
}

func TestHandleMove_DoesNotSendToMover(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{TileX: 5, TileY: 3})
	r.handleMove(mover, payload)
	r.flushMoves()

	_, got := findMoved(drain(mover))
	assert.False(t, got, "mover must not receive their own moved message")
}

func TestHandleMove_PayloadFieldsPreserved(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{
		TileX:     10,
		TileY:     20,
		PX:        320.5,
		PY:        640.25,
		AvatarURL: "https://cdn.example.com/avatar.png",
		Direction: "up",
		Sitting:   true,
	})
	r.handleMove(mover, payload)
	r.flushMoves()

	msgs := drain(other)
	p, ok := findMovedWithAvatarURL(msgs)
	require.True(t, ok, "a moved event with AvatarURL must be broadcast")
	assert.Equal(t, "alice", p.UserID)
	assert.Equal(t, 10, p.TileX)
	assert.Equal(t, 20, p.TileY)
	assert.InDelta(t, 320.5, p.PX, 0.001)
	assert.InDelta(t, 640.25, p.PY, 0.001)
	// When AvatarURL changes a JSON moved event is broadcast immediately —
	// findMovedWithAvatarURL picks that up and it carries the full avatar_url.
	assert.Equal(t, "https://cdn.example.com/avatar.png", p.AvatarURL)
	assert.Equal(t, "up", p.Direction)
	assert.True(t, p.Sitting)
}

func TestHandleMove_AvatarURLChangeBroadcastsJSONImmediately(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	mover.AvatarURL = "https://cdn.example.com/walk.png" // initial walk spritesheet
	other := newTestClient(r, "bob", "available")

	// Simulate player sitting down: avatar_url switches to the sitting spritesheet.
	payload := encodePayload(ClientMovePayload{
		TileX:     3,
		TileY:     3,
		AvatarURL: "https://cdn.example.com/sit.png",
		Sitting:   true,
	})
	r.handleMove(mover, payload)
	// Do NOT call flushMoves — the JSON event must arrive before the binary tick.

	msgs := drain(other)
	p, ok := findMovedWithAvatarURL(msgs)
	require.True(t, ok, "a JSON moved event must be broadcast immediately when AvatarURL changes")
	assert.Equal(t, "https://cdn.example.com/sit.png", p.AvatarURL, "new sit spritesheet URL must be in JSON event")
	assert.True(t, p.Sitting)
}

func TestHandleMove_UnchangedAvatarURLDoesNotBroadcastJSON(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	mover.AvatarURL = "https://cdn.example.com/walk.png"
	other := newTestClient(r, "bob", "available")

	// Move with the same avatar_url — no immediate JSON broadcast expected.
	payload := encodePayload(ClientMovePayload{
		TileX:     5,
		TileY:     5,
		AvatarURL: "https://cdn.example.com/walk.png",
	})
	r.handleMove(mover, payload)
	// Without flushMoves the only message that could arrive is the JSON event.

	msgs := drain(other)
	_, ok := findMoved(msgs)
	assert.False(t, ok, "no moved event must be broadcast before the tick when avatar_url is unchanged")
}

func TestHandleMove_SittingFalseIsExplicitlyBroadcast(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// sitting=false must survive JSON round-trip — MovedPayload.Sitting has no omitempty
	// so standing-up after sitting must be broadcast explicitly to clear the animation state.
	payload := encodePayload(ClientMovePayload{TileX: 1, TileY: 1, Sitting: false})
	r.handleMove(mover, payload)
	r.flushMoves()

	p, ok := findMoved(drain(other))
	require.True(t, ok)
	assert.False(t, p.Sitting, "sitting=false must be broadcast so receivers clear sit state")
}

func TestHandleMove_MultipleMovers_EachReceivesOthers(t *testing.T) {
	r := newTestRoom()
	alice := newTestClient(r, "alice", "available")
	bob := newTestClient(r, "bob", "available")

	r.handleMove(alice, encodePayload(ClientMovePayload{TileX: 1, TileY: 1}))
	r.handleMove(bob, encodePayload(ClientMovePayload{TileX: 9, TileY: 9}))
	r.flushMoves()

	p, ok := findMoved(drain(bob))
	require.True(t, ok, "bob should receive alice's move after tick flush")
	assert.Equal(t, "alice", p.UserID)

	p2, ok2 := findMoved(drain(alice))
	require.True(t, ok2, "alice should receive bob's move after tick flush")
	assert.Equal(t, "bob", p2.UserID)
}

func TestHandleMove_ThirdPartyObserverReceivesMove(t *testing.T) {
	r := newTestRoom()
	alice := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")
	carol := newTestClient(r, "carol", "available")

	r.handleMove(alice, encodePayload(ClientMovePayload{TileX: 2, TileY: 4}))
	r.flushMoves()

	_, got := findMoved(drain(carol))
	assert.True(t, got, "third-party observers should receive moves after tick flush")
}

func TestHandleMove_ConsecutiveMoves_CoalescedToLatest(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// Tick batching: two rapid moves from the same player produce one flush.
	// Intermediate positions are intentionally dropped — only the latest wins.
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 1, TileY: 1}))
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 2, TileY: 2}))
	r.flushMoves()

	msgs := drain(other)
	count := 0
	for _, m := range msgs {
		if m.Type == MsgMoved {
			count++
		}
	}
	assert.Equal(t, 1, count, "tick batching coalesces consecutive moves: only latest position flushed")
	p, ok := findMoved(msgs)
	require.True(t, ok)
	assert.Equal(t, 2, p.TileX)
	assert.Equal(t, 2, p.TileY)
}

func TestHandleMove_TeleportRejected(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// Establish a known position (tile 5,5).
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 5, TileY: 5}))
	r.flushMoves()
	drain(other) // discard first move

	// Attempt a teleport (>3 tiles in one step) — rejected, nothing added to pendingMoves.
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 20, TileY: 20}))
	r.flushMoves()

	_, got := findMoved(drain(other))
	assert.False(t, got, "teleport (>3 tile jump) must be rejected and not broadcast")
}
