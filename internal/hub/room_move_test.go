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

// ── handleMove — immediate broadcast ─────────────────────────────────────────

func TestHandleMove_BroadcastsImmediately(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{TileX: 5, TileY: 3, PX: 160, PY: 96, Direction: "right"})
	r.handleMove(mover, payload)

	// Moved must be broadcast right away — no explicit flush step needed.
	p, ok := findMoved(drain(other))
	require.True(t, ok, "moved must be broadcast immediately to peers")
	assert.Equal(t, 5, p.TileX)
	assert.Equal(t, 3, p.TileY)
}

func TestHandleMove_DoesNotSendToMover(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{TileX: 5, TileY: 3})
	r.handleMove(mover, payload)

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

	p, ok := findMoved(drain(other))
	require.True(t, ok)
	assert.Equal(t, "alice", p.UserID)
	assert.Equal(t, 10, p.TileX)
	assert.Equal(t, 20, p.TileY)
	assert.InDelta(t, 320.5, p.PX, 0.001)
	assert.InDelta(t, 640.25, p.PY, 0.001)
	assert.Equal(t, "https://cdn.example.com/avatar.png", p.AvatarURL)
	assert.Equal(t, "up", p.Direction)
	assert.True(t, p.Sitting)
}

func TestHandleMove_SittingFalseIsExplicitlyBroadcast(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// sitting=false must survive JSON round-trip — MovedPayload.Sitting has no omitempty
	// so standing-up after sitting must be broadcast explicitly to clear the animation state.
	payload := encodePayload(ClientMovePayload{TileX: 1, TileY: 1, Sitting: false})
	r.handleMove(mover, payload)

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

	// bob receives alice's move
	p, ok := findMoved(drain(bob))
	require.True(t, ok, "bob should receive alice's move immediately")
	assert.Equal(t, "alice", p.UserID)

	// alice receives bob's move
	p2, ok2 := findMoved(drain(alice))
	require.True(t, ok2, "alice should receive bob's move immediately")
	assert.Equal(t, "bob", p2.UserID)
}

func TestHandleMove_ThirdPartyObserverReceivesMove(t *testing.T) {
	r := newTestRoom()
	alice := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")
	carol := newTestClient(r, "carol", "available")

	r.handleMove(alice, encodePayload(ClientMovePayload{TileX: 2, TileY: 4}))

	_, got := findMoved(drain(carol))
	assert.True(t, got, "third-party observers should receive moves immediately")
}

func TestHandleMove_ConsecutiveMoves_AllBroadcast(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// With immediate broadcast every move is delivered — no overwriting like the old buffer.
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 1, TileY: 1}))
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 2, TileY: 2}))

	msgs := drain(other)
	count := 0
	for _, m := range msgs {
		if m.Type == MsgMoved {
			count++
		}
	}
	assert.Equal(t, 2, count, "both move events must be delivered without dropping the first")
}

func TestHandleMove_TeleportRejected(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// Establish a known position (tile 5,5).
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 5, TileY: 5}))
	drain(other) // discard first move

	// Attempt a teleport (>3 tiles in one step).
	r.handleMove(mover, encodePayload(ClientMovePayload{TileX: 20, TileY: 20}))

	_, got := findMoved(drain(other))
	assert.False(t, got, "teleport (>3 tile jump) must be rejected and not broadcast")
}
