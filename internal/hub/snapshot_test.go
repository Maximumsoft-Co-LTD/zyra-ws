package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAOIMoveReportsCellChange(t *testing.T) {
	g := NewAOIGrid()
	c := &Client{UserID: "a"}
	assert.True(t, g.Move(c, 0, 0), "first placement is a cell change")
	assert.False(t, g.Move(c, 1, 1), "same cell → no change")
	assert.True(t, g.Move(c, AOICellSize*2, 0), "crossing into a new cell → change")
}

// movedUserIDs extracts the user_ids of all `moved` envelopes a client received.
func movedUserIDs(t *testing.T, c *Client) []string {
	t.Helper()
	var ids []string
	for _, e := range drain(c) {
		if e.Type != MsgMoved {
			continue
		}
		var p MovedPayload
		require.NoError(t, json.Unmarshal(e.Payload, &p))
		ids = append(ids, p.UserID)
	}
	return ids
}

func TestSendNeighborSnapshot_IncludesStationarySkipsMoving(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5) // adjacent → same AOI neighbourhood
	r.aoi.Move(a, 5, 5)
	r.aoi.Move(b, 6, 5)
	drain(b) // clear any join noise

	// a is standing still → b's appear-snapshot must include a's current position.
	r.sendNeighborSnapshot(b)
	assert.Contains(t, movedUserIDs(t, b), "a", "stationary neighbour must be sent to the mover")

	// a is now mid-walk → it is covered by the room-wide moving broadcast, so the
	// appear-snapshot must NOT also push a stale static position for it.
	a.IsMoving = true
	drain(b)
	r.sendNeighborSnapshot(b)
	assert.NotContains(t, movedUserIDs(t, b), "a", "moving neighbour must be skipped")
}

func TestSendNeighborSnapshot_RoomIsolation(t *testing.T) {
	r := newTestRoom()
	a := newTestClient(r, "a", "available")
	b := newTestClient(r, "b", "available")
	setTile(a, 5, 5)
	setTile(b, 6, 5)
	r.aoi.Move(a, 5, 5)
	r.aoi.Move(b, 6, 5)

	// a is inside a private room; b is on the open floor — they must not reconcile
	// each other even though their AOI cells overlap.
	a.RoomID = "room-1"
	drain(b)
	r.sendNeighborSnapshot(b)
	assert.NotContains(t, movedUserIDs(t, b), "a", "peer in a different room must be skipped")

	// Same room → reconciled.
	b.RoomID = "room-1"
	drain(b)
	r.sendNeighborSnapshot(b)
	assert.Contains(t, movedUserIDs(t, b), "a", "same-room peer must be included")
}
