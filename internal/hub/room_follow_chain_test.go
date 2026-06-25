package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// firstPayload finds the first message of msgType and unmarshals its payload into dest.
func firstPayload(t *testing.T, msgs []Envelope, msgType string, dest any) bool {
	t.Helper()
	for _, m := range msgs {
		if m.Type == msgType {
			require.NoError(t, json.Unmarshal(m.Payload, dest))
			return true
		}
	}
	return false
}

// linkChain wires a follow chain head <- ... <- tail using the in-memory pointers,
// mirroring what a sequence of handleFollow calls would produce.
func linkChain(nodes ...*Client) {
	for i := 0; i < len(nodes)-1; i++ {
		ahead, behind := nodes[i], nodes[i+1]
		ahead.FollowerID = behind.UserID
		behind.FollowTargetID = ahead.UserID
	}
}

// ── append-to-tail ──────────────────────────────────────────────────────────

func TestHandleFollow_AppendsToChainTail(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	linkChain(leader, f1) // leader <- f1

	alice := newTestClient(r, "alice", "available")
	r.handleFollow(alice, encodePayload(ClientFollowPayload{TargetUserID: "leader"}))

	// Alice must be appended behind the TAIL (f1), not bound to the clicked leader.
	assert.Equal(t, "f1", alice.FollowTargetID, "alice should follow the tail (f1), not the leader")
	assert.Equal(t, "alice", f1.FollowerID, "f1 should gain alice as its follower")

	// Alice is told the resolved node to walk behind + the chain leader.
	var ap FollowAssignedPayload
	require.True(t, firstPayload(t, drain(alice), MsgFollowAssigned, &ap), "alice should receive follow_assigned")
	assert.Equal(t, "f1", ap.TargetUserID)
	assert.Equal(t, "leader", ap.ChainLeader)

	// The tail learns it gained a direct follower.
	assert.True(t, hasType(drain(f1), MsgFollowStarted), "tail (f1) should receive follow_started")
}

// ── self-healing: middle node leaves ──────────────────────────────────────────

func TestDetach_MiddleNodeHealsChain(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	f2 := newTestClient(r, "f2", "available")
	f3 := newTestClient(r, "f3", "available")
	linkChain(leader, f1, f2, f3) // leader <- f1 <- f2 <- f3

	r.detach(f2)

	// f2 fully unlinked.
	assert.Equal(t, "", f2.FollowTargetID)
	assert.Equal(t, "", f2.FollowerID)
	// Gap healed: f1 <- f3 now adjacent.
	assert.Equal(t, "f3", f1.FollowerID, "f1 should now be followed by f3")
	assert.Equal(t, "f1", f3.FollowTargetID, "f3 should now follow f1 (the upstream neighbour)")

	// f3 is retargeted to f1, chain leader unchanged.
	var ap FollowAssignedPayload
	require.True(t, firstPayload(t, drain(f3), MsgFollowAssigned, &ap), "f3 should receive follow_assigned")
	assert.Equal(t, "f1", ap.TargetUserID)
	assert.Equal(t, "leader", ap.ChainLeader)
}

func TestUnregister_HealsChainOnDisconnect(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	f2 := newTestClient(r, "f2", "available")
	f3 := newTestClient(r, "f3", "available")
	linkChain(leader, f1, f2, f3)

	// f2 disconnects.
	r.unregister(f2)

	assert.Equal(t, "f3", f1.FollowerID, "chain should heal: f1 followed by f3")
	assert.Equal(t, "f1", f3.FollowTargetID, "chain should heal: f3 follows f1")
	assert.True(t, hasType(drain(f3), MsgFollowAssigned), "f3 should be retargeted after f2 disconnects")
}

// ── head (leader) leaves: trailing node is told to stop ───────────────────────

func TestDetach_HeadLeaves_TrailingNodeRevoked(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	linkChain(leader, f1) // leader <- f1

	r.detach(leader)

	assert.Equal(t, "", f1.FollowTargetID, "f1 has no one ahead after the leader leaves")
	assert.True(t, hasType(drain(f1), MsgFollowRevoked), "f1 should be told to stop following")
}

// ── cycle prevention (detach-first) ───────────────────────────────────────────

func TestHandleFollow_FollowingOwnFollower_NoCycle(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	linkChain(leader, f1) // leader <- f1  (f1 follows leader)

	// leader now tries to follow its own follower f1 — must not form a cycle.
	r.handleFollow(leader, encodePayload(ClientFollowPayload{TargetUserID: "f1"}))

	assert.Equal(t, "f1", leader.FollowTargetID, "leader now follows f1")
	assert.Equal(t, "leader", f1.FollowerID, "f1 is followed by leader")
	assert.Equal(t, "", f1.FollowTargetID, "f1 no longer follows leader (detached first) — no cycle")

	// Traversals must terminate (would hang if a cycle existed).
	assert.Equal(t, "f1", r.resolveLeader(leader).UserID)
	assert.Equal(t, "leader", r.resolveTail(f1).UserID)
}

// ── traversal safety against a corrupt self-cycle ─────────────────────────────

func TestResolveTail_TerminatesOnCorruptCycle(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	a.FollowerID = "a" // corrupt self-reference

	// Must terminate (bounded by maxFollowChain) rather than spin forever.
	done := make(chan *Client, 1)
	go func() { done <- r.resolveTail(a) }()
	select {
	case got := <-done:
		assert.Equal(t, "a", got.UserID)
	default:
		// resolveTail is synchronous and fast; if we reach here it already returned.
	}
	assert.Equal(t, "a", r.resolveTail(a).UserID, "resolveTail must terminate on a self-cycle")
}

// ── followee kicks a follower: chain heals + follower stopped ─────────────────

func TestHandleStopFollower_KicksAndHeals(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	f2 := newTestClient(r, "f2", "available")
	linkChain(leader, f1, f2) // leader <- f1 <- f2

	// leader kicks its direct follower f1.
	r.handleStopFollower(leader, encodePayload(ClientStopFollowerPayload{FollowerUserID: "f1"}))

	assert.Equal(t, "", f1.FollowTargetID, "kicked follower leaves the chain")
	assert.Equal(t, "f2", leader.FollowerID, "chain heals: leader followed by f2")
	assert.Equal(t, "leader", f2.FollowTargetID, "chain heals: f2 follows leader")
	assert.True(t, hasType(drain(f1), MsgFollowRevoked), "kicked follower receives follow_revoked")
}

// ── server-authoritative follow (hidden node) ────────────────────────────────

// movingFor finds a `moving` broadcast addressed to a specific user_id.
func movingFor(t *testing.T, msgs []Envelope, userID string) (MovingPayload, bool) {
	t.Helper()
	for _, m := range msgs {
		if m.Type != MsgMoving {
			continue
		}
		var mp MovingPayload
		if err := json.Unmarshal(m.Payload, &mp); err == nil && mp.UserID == userID {
			return mp, true
		}
	}
	return MovingPayload{}, false
}

func TestCascade_DrivesHiddenFollowerOneTileBehind(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "C", "available")
	b := newTestClient(r, "B", "available")
	linkChain(leader, b) // C <- B  (B follows C)
	observer := newTestClient(r, "obs", "available")

	leader.TileX, leader.TileY = 5, 5
	b.TileX, b.TileY = 5, 6 // one tile behind C
	b.Hidden = true         // B's tab is backgrounded → server drives it

	// C walks (5,5) → (6,5) → (7,5).
	path := []TilePoint{{TileX: 5, TileY: 5}, {TileX: 6, TileY: 5}, {TileX: 7, TileY: 5}}
	r.handleMoveTo(leader, encodePayload(ClientMoveToPayload{Path: path}))

	// B is server-walked onto C's vacated tiles, ending one tile behind C (at (6,5)).
	assert.Equal(t, 6, b.TileX, "hidden B should be driven forward by the server")
	assert.Equal(t, 5, b.TileY)

	// Peers receive a server-originated "moving" for B even though B never sent one.
	mp, ok := movingFor(t, drain(observer), "B")
	require.True(t, ok, "observer should receive a server-driven moving for hidden B")
	assert.Equal(t, 6, mp.Path[len(mp.Path)-1].TileX, "B's path ends one tile behind C")
}

func TestCascade_SkipsForegroundFollower(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "C", "available")
	b := newTestClient(r, "B", "available")
	linkChain(leader, b)
	leader.TileX, leader.TileY = 5, 5
	b.TileX, b.TileY = 5, 6
	b.Hidden = false // foreground → B drives itself, server must NOT move it

	r.handleMoveTo(leader, encodePayload(ClientMoveToPayload{
		Path: []TilePoint{{TileX: 5, TileY: 5}, {TileX: 6, TileY: 5}},
	}))

	assert.Equal(t, 5, b.TileX, "foreground follower must not be server-driven")
	assert.Equal(t, 6, b.TileY)
}

func TestHandleMoveTo_HiddenFollowerSelfMoveRejected(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "C", "available")
	b := newTestClient(r, "B", "available")
	linkChain(leader, b)
	b.TileX, b.TileY = 5, 6
	b.Hidden = true

	// A stale self-move from the hidden follower must be ignored (server owns it).
	r.handleMoveTo(b, encodePayload(ClientMoveToPayload{
		Path: []TilePoint{{TileX: 5, TileY: 6}, {TileX: 9, TileY: 9}},
	}))

	assert.Equal(t, 5, b.TileX, "hidden follower's own move_to must be rejected")
	assert.Equal(t, 6, b.TileY)
}

func TestHandleStopFollower_NonDirectFollower_NoOp(t *testing.T) {
	r := newTestRoom()
	leader := newTestClient(r, "leader", "available")
	f1 := newTestClient(r, "f1", "available")
	f2 := newTestClient(r, "f2", "available")
	linkChain(leader, f1, f2) // leader <- f1 <- f2

	// leader tries to kick f2, which does NOT directly follow leader → no-op.
	r.handleStopFollower(leader, encodePayload(ClientStopFollowerPayload{FollowerUserID: "f2"}))

	assert.Equal(t, "f1", f2.FollowTargetID, "f2 still follows f1 (not kicked)")
	assert.Equal(t, "f2", f1.FollowerID, "chain unchanged")
}
