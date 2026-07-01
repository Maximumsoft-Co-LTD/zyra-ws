package hub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setTile positions a test client for the clustering math.
func setTile(c *Client, x, y int) {
	c.TileX = x
	c.TileY = y
}

// onlySession returns the single session in the room (fails if not exactly one).
func onlySession(t *testing.T, r *Room) *ChatSession {
	t.Helper()
	require.Len(t, r.chatSessions, 1)
	for _, s := range r.chatSessions {
		return s
	}
	return nil
}

func TestChatSpace_AutoFormAdjacentPair(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5) // Chebyshev 1 → adjacent

	r.recomputeChatSessions()

	s := onlySession(t, r)
	assert.ElementsMatch(t, []string{"a", "b"}, s.Members)
	assert.Equal(t, s.ID, r.playerSession["a"])
	assert.Equal(t, s.ID, r.playerSession["b"])
}

func TestChatSpace_NoSessionWhenFarApart(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 8, 5) // Chebyshev 3 → not adjacent

	r.recomputeChatSessions()

	assert.Empty(t, r.chatSessions)
}

func TestChatSpace_BusyOrAwayExcluded(t *testing.T) {
	for _, status := range []string{"busy", "away"} {
		t.Run(status, func(t *testing.T) {
			r := newTestRoom()
			a := newTestClient(r, "a", "available")
			b := newTestClient(r, "b", status)
			setTile(a, 5, 5)
			setTile(b, 6, 5)

			r.recomputeChatSessions()

			assert.Empty(t, r.chatSessions)
		})
	}
}

func TestChatSpace_TwoPersonEndsAfterDebounce(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	r.recomputeChatSessions()
	require.Len(t, r.chatSessions, 1)

	// b walks far away — first recompute starts the grace timer but keeps both.
	setTile(b, 20, 20)
	r.recomputeChatSessions()
	assert.Len(t, r.chatSessions, 1, "session should survive the debounce window")

	// Simulate the debounce elapsing, then recompute → session dissolves.
	for uid := range r.chatMemberLeaveAt {
		r.chatMemberLeaveAt[uid] = time.Now().Add(-chatEndDebounce - time.Millisecond)
	}
	r.recomputeChatSessions()
	assert.Empty(t, r.chatSessions)
	assert.Empty(t, r.playerSession)
}

func TestChatSpace_MemberLeavesAfterGrace(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	c := newTestClient(r, "c", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	setTile(c, 7, 5)
	r.recomputeChatSessions()
	s := onlySession(t, r)
	require.ElementsMatch(t, []string{"a", "b", "c"}, s.Members)

	// c walks off — kept during the 1s grace.
	setTile(c, 20, 20)
	r.recomputeChatSessions()
	assert.Contains(t, onlySession(t, r).Members, "c", "member kept during grace (no flicker)")

	// Grace elapses → c is removed, session of {a,b} survives, c gets a rejoin window.
	r.chatMemberLeaveAt["c"] = time.Now().Add(-chatLeaveGrace - time.Millisecond)
	r.recomputeChatSessions()
	s = onlySession(t, r)
	assert.ElementsMatch(t, []string{"a", "b"}, s.Members)
	_, hasWindow := r.chatRecentlyLeft["c"]
	assert.True(t, hasWindow, "removed member should get a rejoin window")
}

func TestChatSpace_RejoinWithinWindow(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	c := newTestClient(r, "c", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	setTile(c, 20, 20)
	r.recomputeChatSessions()
	s := onlySession(t, r)
	sid := s.ID

	// Seed a rejoin window for c pointing at the live session.
	r.chatRecentlyLeft["c"] = chatRecentLeave{sessionID: sid, until: time.Now().Add(chatRejoinWindow)}

	// c returns adjacent to b → rejoins the SAME session without asking.
	setTile(c, 7, 5)
	r.recomputeChatSessions()

	s = onlySession(t, r)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, s.Members)
	assert.Equal(t, sid, s.ID, "rejoined the same session id")
	_, stillPending := r.chatRecentlyLeft["c"]
	assert.False(t, stillPending, "rejoin window cleared after rejoin")
}

func TestChatSpace_RejoinWindowExpired(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	c := newTestClient(r, "c", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	setTile(c, 7, 5) // adjacent, but window is expired
	r.recomputeChatSessions()
	s := onlySession(t, r)

	// Pretend c is a recent leaver whose window already closed.
	r.playerSession = map[string]string{"a": s.ID, "b": s.ID}
	s.Members = []string{"a", "b"}
	r.chatRecentlyLeft["c"] = chatRecentLeave{sessionID: s.ID, until: time.Now().Add(-time.Second)}

	r.recomputeChatSessions()

	s = onlySession(t, r)
	assert.ElementsMatch(t, []string{"a", "b"}, s.Members, "expired window → no auto-rejoin")
	_, stillPending := r.chatRecentlyLeft["c"]
	assert.False(t, stillPending, "expired window is cleaned up")
}

func TestChatSpace_AddAndRemoveMember(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	r.recomputeChatSessions()
	sid := onlySession(t, r).ID

	require.True(t, r.addToChatSession(sid, "c"))
	assert.ElementsMatch(t, []string{"a", "b", "c"}, r.chatSessions[sid].Members)
	assert.False(t, r.addToChatSession(sid, "c"), "adding an existing member is a no-op")

	r.removeFromChatSession("c")
	assert.ElementsMatch(t, []string{"a", "b"}, r.chatSessions[sid].Members)

	// Removing below the minimum dissolves the session.
	r.removeFromChatSession("a")
	assert.Empty(t, r.chatSessions)
}

func TestChatSpace_LeaveSuppressesReformUntilWalkAway(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	r.recomputeChatSessions()
	require.Len(t, r.chatSessions, 1)

	// a explicitly leaves → session dissolves and a is suppressed from b.
	r.leaveChatSession("a")
	assert.Empty(t, r.chatSessions)

	// While still adjacent, auto-form must NOT reform the a–b session.
	r.recomputeChatSessions()
	assert.Empty(t, r.chatSessions, "suppressed leaver should not re-cluster while adjacent")

	// a walks away → suppression clears.
	setTile(a, 20, 20)
	r.recomputeChatSessions()
	assert.NotContains(t, r.chatSuppress, "a", "suppression cleared after walking away")

	// a returns adjacent → a fresh session forms again.
	setTile(a, 6, 6)
	r.recomputeChatSessions()
	assert.Len(t, r.chatSessions, 1, "re-approaching after walking away forms a new session")
}

func TestChatSpace_BFSClustersAndConnectivity(t *testing.T) {
	pts := []chatPos{
		{userID: "a", tx: 0, ty: 0},
		{userID: "b", tx: 1, ty: 0}, // adjacent to a
		{userID: "c", tx: 10, ty: 10},
		{userID: "d", tx: 11, ty: 10}, // adjacent to c
	}
	clusters := bfsClusters(pts, nil)
	require.Len(t, clusters, 2)

	positions := map[string]chatPos{}
	for _, p := range pts {
		positions[p.userID] = p
	}
	// a,b,c together: only a,b are connected → largest component is {a,b}.
	keep := largestConnectedComponent([]string{"a", "b", "c"}, positions)
	assert.ElementsMatch(t, []string{"a", "b"}, keep)

	assert.True(t, adjacent(chatPos{tx: 0, ty: 0}, chatPos{tx: 1, ty: 1}))
	assert.False(t, adjacent(chatPos{tx: 0, ty: 0}, chatPos{tx: 2, ty: 0}))
}
