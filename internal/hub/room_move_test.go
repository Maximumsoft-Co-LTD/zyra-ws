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

// storeMove writes a position snapshot directly into pendingMoves, bypassing handleMove.
// Use when the test only cares about flush behaviour, not move validation.
func storeMove(r *Room, p MovedPayload) {
	r.pendingMoves.Store(p.UserID, p)
}

// ── handleMove ────────────────────────────────────────────────────────────────

func TestHandleMove_QueuesPositionWithoutImmediateBroadcast(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{TileX: 5, TileY: 3, PX: 160, PY: 96, Direction: "right"})
	r.handleMove(mover, payload)

	// No moved message yet — it must be queued, not broadcast immediately.
	assert.False(t, hasType(drain(other), MsgMoved), "moved must not be broadcast before flush")

	// The pending move must be present with the correct tile.
	v, ok := r.pendingMoves.Load("alice")
	require.True(t, ok, "pending move should be stored after handleMove")
	pending := v.(MovedPayload)
	assert.Equal(t, 5, pending.TileX)
	assert.Equal(t, 3, pending.TileY)
}

func TestHandleMove_OverwritesPreviousPendingMove(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")

	// Two moves before any flush — only the second should survive in the queue.
	payload1 := encodePayload(ClientMovePayload{TileX: 1, TileY: 1})
	payload2 := encodePayload(ClientMovePayload{TileX: 2, TileY: 2})
	r.handleMove(mover, payload1)
	r.handleMove(mover, payload2)

	v, ok := r.pendingMoves.Load("alice")
	require.True(t, ok)
	pending := v.(MovedPayload)
	assert.Equal(t, 2, pending.TileX, "second move should overwrite the first in the pending queue")
	assert.Equal(t, 2, pending.TileY)
}

// ── flushPendingMoves ─────────────────────────────────────────────────────────

func TestFlushPendingMoves_BroadcastsToOthersNotSelf(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	storeMove(r, MovedPayload{UserID: "alice", TileX: 4, TileY: 7})
	r.flushPendingMoves()

	_, got := findMoved(drain(other))
	assert.True(t, got, "other client should receive moved after flush")

	_, selfGot := findMoved(drain(mover))
	assert.False(t, selfGot, "mover must not receive their own moved message")
}

func TestFlushPendingMoves_PayloadFieldsPreserved(t *testing.T) {
	r := newTestRoom()
	newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	storeMove(r, MovedPayload{
		UserID:    "alice",
		TileX:     10,
		TileY:     20,
		PX:        320.5,
		PY:        640.25,
		AvatarURL: "https://cdn.example.com/avatar.png",
		Direction: "up",
		Sitting:   true,
	})
	r.flushPendingMoves()

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

func TestFlushPendingMoves_SittingFalseIsExplicitlyBroadcast(t *testing.T) {
	r := newTestRoom()
	newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// sitting=false must survive JSON round-trip — MovedPayload.Sitting has no omitempty
	// so standing-up after sitting must be broadcast explicitly to clear the animation state.
	storeMove(r, MovedPayload{UserID: "alice", TileX: 1, TileY: 1, Sitting: false})
	r.flushPendingMoves()

	p, ok := findMoved(drain(other))
	require.True(t, ok)
	assert.False(t, p.Sitting, "sitting=false must be broadcast so receivers clear the sit state")
}

func TestFlushPendingMoves_MultipleMovers(t *testing.T) {
	r := newTestRoom()
	alice := newTestClient(r, "alice", "available")
	bob := newTestClient(r, "bob", "available")

	storeMove(r, MovedPayload{UserID: "alice", TileX: 1, TileY: 1})
	storeMove(r, MovedPayload{UserID: "bob", TileX: 9, TileY: 9})
	r.flushPendingMoves()

	// bob receives alice's move only
	p, ok := findMoved(drain(bob))
	require.True(t, ok, "bob should receive alice's move")
	assert.Equal(t, "alice", p.UserID)

	// alice receives bob's move only
	p2, ok2 := findMoved(drain(alice))
	require.True(t, ok2, "alice should receive bob's move")
	assert.Equal(t, "bob", p2.UserID)
}

func TestFlushPendingMoves_DisconnectedPlayerSkipped(t *testing.T) {
	r := newTestRoom()
	newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	// Queue a move for alice, then simulate alice disconnecting before the 50ms tick.
	storeMove(r, MovedPayload{UserID: "alice", TileX: 3, TileY: 3})
	r.clients.Delete("alice")

	r.flushPendingMoves()

	// bob must NOT receive alice's move — it would arrive after "left", creating a ghost.
	_, got := findMoved(drain(other))
	assert.False(t, got, "disconnected player's buffered move must not be broadcast")
}

func TestFlushPendingMoves_ClearsPendingAfterFlush(t *testing.T) {
	r := newTestRoom()
	newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	storeMove(r, MovedPayload{UserID: "alice", TileX: 5, TileY: 5})
	r.flushPendingMoves()
	drain(other) // discard first batch

	r.flushPendingMoves() // second flush — queue is empty
	_, got := findMoved(drain(other))
	assert.False(t, got, "second flush must not re-broadcast already-sent moves")
}

func TestFlushPendingMoves_EmptyQueueSendsNothing(t *testing.T) {
	r := newTestRoom()
	newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	r.flushPendingMoves()

	assert.Empty(t, drain(other), "no messages should be sent when pending queue is empty")
}

func TestFlushPendingMoves_ThirdPartyObserverReceivesMove(t *testing.T) {
	r := newTestRoom()
	newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")
	carol := newTestClient(r, "carol", "available")

	storeMove(r, MovedPayload{UserID: "alice", TileX: 2, TileY: 4})
	r.flushPendingMoves()

	_, got := findMoved(drain(carol))
	assert.True(t, got, "all connected peers including third-party observers should receive the move")
}

func TestHandleMove_ThenFlush_DeliversCorrectPayload(t *testing.T) {
	r := newTestRoom()
	mover := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientMovePayload{
		TileX:     6,
		TileY:     8,
		PX:        192.0,
		PY:        256.0,
		Direction: "left",
		Sitting:   false,
		AvatarURL: "https://cdn.example.com/walk.png",
	})
	r.handleMove(mover, payload)

	// No broadcast yet.
	assert.Empty(t, drain(other))

	// After flush, bob receives the correct snapshot.
	r.flushPendingMoves()
	p, ok := findMoved(drain(other))
	require.True(t, ok)
	assert.Equal(t, "alice", p.UserID)
	assert.Equal(t, 6, p.TileX)
	assert.Equal(t, 8, p.TileY)
	assert.Equal(t, "left", p.Direction)
	assert.Equal(t, "https://cdn.example.com/walk.png", p.AvatarURL)
}
