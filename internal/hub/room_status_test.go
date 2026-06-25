package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── test helpers ──────────────────────────────────────────────────────────────

func newTestRoom() *Room {
	h := &Hub{}
	r := &Room{workspaceID: "ws-test", hub: h, aoi: NewAOIGrid(), stopTick: make(chan struct{})}
	return r
}

func newTestClient(r *Room, userID, status string) *Client {
	c := &Client{
		hub:         r.hub,
		room:        r,
		send:        make(chan []byte, 256),
		sendBin:     make(chan []byte, 256),
		UserID:      userID,
		DisplayName: userID,
		Status:      status,
	}
	r.clients.Store(userID, c)
	r.aoi.Move(c, c.TileX, c.TileY) // seed AOI grid so handleMove AOI broadcast finds this client
	return c
}

// drain reads all buffered messages from a client's send channels (non-blocking).
// Binary moved_bin frames from sendBin are decoded and converted to Envelope so
// that findMoved() and hasType() work identically for both text and binary paths.
func drain(c *Client) []Envelope {
	var out []Envelope
	for {
		select {
		case b := <-c.send:
			var e Envelope
			if err := json.Unmarshal(b, &e); err == nil {
				out = append(out, e)
			}
		case b := <-c.sendBin:
			if p := decodeBinMoved(b); p != nil {
				raw, _ := json.Marshal(p)
				out = append(out, Envelope{Type: MsgMoved, Payload: raw})
			}
		default:
			return out
		}
	}
}

func hasType(msgs []Envelope, msgType string) bool {
	for _, m := range msgs {
		if m.Type == msgType {
			return true
		}
	}
	return false
}

func encodePayload(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ── handleStatus ──────────────────────────────────────────────────────────────

func TestHandleStatus_ValidStatuses(t *testing.T) {
	statuses := []string{"available", "busy", "away", "dnd"}
	for _, status := range statuses {
		t.Run(status, func(t *testing.T) {
			r := newTestRoom()
			sender := newTestClient(r, "u1", "available")
			other := newTestClient(r, "u2", "available")

			payload := encodePayload(ClientStatusPayload{Status: status, CustomMsg: ""})
			r.handleStatus(sender, payload)

			assert.Equal(t, status, sender.Status, "sender.Status should be updated")

			// other receives status_changed
			msgs := drain(other)
			assert.True(t, hasType(msgs, MsgStatusChanged), "other should receive status_changed")

			// sender does NOT receive status_changed (broadcastExcept)
			senderMsgs := drain(sender)
			assert.False(t, hasType(senderMsgs, MsgStatusChanged), "sender should not receive its own status_changed")
		})
	}
}

func TestHandleStatus_InvalidStatus(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "u1", "available")

	payload := encodePayload(ClientStatusPayload{Status: "invisible"})
	r.handleStatus(sender, payload)

	msgs := drain(sender)
	require.True(t, hasType(msgs, MsgError), "should receive error for invalid status")
	assert.Equal(t, "available", sender.Status, "status should not change on invalid input")
}

func TestHandleStatus_CustomMsgTruncatedAt30Runes(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "u1", "available")

	longMsg := "あいうえおかきくけこさしすせそたちつてとなにぬねのはひ" // 28 runes, safe
	tooLong := longMsg + "ふへほまみ"             // 33 runes, over limit

	payload := encodePayload(ClientStatusPayload{Status: "busy", CustomMsg: tooLong})
	r.handleStatus(sender, payload)

	assert.Equal(t, 30, len([]rune(sender.CustomMsg)), "custom_msg should be truncated to 30 runes")
}

func TestHandleStatus_CustomMsgExactly30RunesPassesThrough(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "u1", "available")

	exactly30 := "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほ" // exactly 30 runes
	payload := encodePayload(ClientStatusPayload{Status: "away", CustomMsg: exactly30})
	r.handleStatus(sender, payload)

	assert.Equal(t, exactly30, sender.CustomMsg)
}

func TestHandleStatus_StatusChangedPayloadContainsCorrectFields(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	other := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientStatusPayload{Status: "dnd", CustomMsg: "in a meeting"})
	r.handleStatus(sender, payload)

	msgs := drain(other)
	require.True(t, hasType(msgs, MsgStatusChanged))

	// find the status_changed message and check its payload
	for _, m := range msgs {
		if m.Type != MsgStatusChanged {
			continue
		}
		var p StatusChangedPayload
		require.NoError(t, json.Unmarshal(m.Payload, &p))
		assert.Equal(t, "alice", p.UserID)
		assert.Equal(t, "dnd", p.Status)
		assert.Equal(t, "in a meeting", p.CustomMsg)
	}
}

// ── handleWave ────────────────────────────────────────────────────────────────

func TestHandleWave_TargetDND_AnimationBroadcastNotificationSuppressed(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	target := newTestClient(r, "bob", "dnd")

	payload := encodePayload(ClientWavePayload{TargetUserID: "bob"})
	r.handleWave(sender, payload)

	// wave_animation goes to everyone (including sender and target)
	senderMsgs := drain(sender)
	targetMsgs := drain(target)

	assert.True(t, hasType(senderMsgs, MsgWaveAnimation), "sender should see wave animation")
	assert.True(t, hasType(targetMsgs, MsgWaveAnimation), "dnd target should still see wave animation")

	// wave_received must NOT be delivered to a dnd target
	assert.False(t, hasType(targetMsgs, MsgWaveReceived), "dnd target must not get wave notification")
}

func TestHandleWave_TargetBusy_AnimationBroadcastNotificationSuppressed(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	target := newTestClient(r, "bob", "busy")

	payload := encodePayload(ClientWavePayload{TargetUserID: "bob"})
	r.handleWave(sender, payload)

	senderMsgs := drain(sender)
	targetMsgs := drain(target)

	assert.True(t, hasType(senderMsgs, MsgWaveAnimation), "sender should see wave animation")
	assert.True(t, hasType(targetMsgs, MsgWaveAnimation), "busy target should still see wave animation")
	assert.False(t, hasType(targetMsgs, MsgWaveReceived), "busy target must not get wave notification")
}

func TestHandleWave_TargetAvailable_BothAnimationAndNotification(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	target := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientWavePayload{TargetUserID: "bob"})
	r.handleWave(sender, payload)

	targetMsgs := drain(target)
	assert.True(t, hasType(targetMsgs, MsgWaveAnimation))
	assert.True(t, hasType(targetMsgs, MsgWaveReceived), "available target should get wave notification")
}

func TestHandleWave_TargetAway_BothAnimationAndNotification(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	target := newTestClient(r, "bob", "away")

	payload := encodePayload(ClientWavePayload{TargetUserID: "bob"})
	r.handleWave(sender, payload)

	targetMsgs := drain(target)
	assert.True(t, hasType(targetMsgs, MsgWaveAnimation))
	assert.True(t, hasType(targetMsgs, MsgWaveReceived), "away target should still get wave notification")
}

func TestHandleWave_WaveAnimationBroadcastToAllIncludingThirdParty(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "dnd")
	observer := newTestClient(r, "carol", "available")

	payload := encodePayload(ClientWavePayload{TargetUserID: "bob"})
	r.handleWave(sender, payload)

	assert.True(t, hasType(drain(observer), MsgWaveAnimation), "third-party observer should see wave animation")
}

func TestHandleWave_SelfWave_Error(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	payload := encodePayload(ClientWavePayload{TargetUserID: "alice"})
	r.handleWave(sender, payload)

	assert.True(t, hasType(drain(sender), MsgError))
}

func TestHandleWave_EmptyTargetID_Error(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	payload := encodePayload(ClientWavePayload{TargetUserID: ""})
	r.handleWave(sender, payload)

	assert.True(t, hasType(drain(sender), MsgError))
}

func TestHandleWave_TargetNotInRoom_Error(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	payload := encodePayload(ClientWavePayload{TargetUserID: "ghost"})
	r.handleWave(sender, payload)

	assert.True(t, hasType(drain(sender), MsgError))
}

// ── handleFollow ──────────────────────────────────────────────────────────────

func TestHandleFollow_TargetBusy_Error(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "busy")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "bob"})
	r.handleFollow(follower, payload)

	assert.True(t, hasType(drain(follower), MsgError), "following a busy user should return error")
}

func TestHandleFollow_TargetDND_Error(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "dnd")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "bob"})
	r.handleFollow(follower, payload)

	assert.True(t, hasType(drain(follower), MsgError), "following a dnd user should return error")
}

func TestHandleFollow_TargetAvailable_FollowStartedSentToTarget(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")
	target := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "bob"})
	r.handleFollow(follower, payload)

	assert.True(t, hasType(drain(target), MsgFollowStarted), "available target should receive follow_started")
	assert.Equal(t, "bob", follower.FollowTargetID)
}

func TestHandleFollow_TargetAway_FollowAllowed(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")
	awayTarget := newTestClient(r, "bob", "away")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "bob"})
	r.handleFollow(follower, payload)

	assert.True(t, hasType(drain(awayTarget), MsgFollowStarted), "away target should receive follow_started (away ≠ dnd/busy)")
	assert.False(t, hasType(drain(follower), MsgError))
}

func TestHandleFollow_SelfFollow_Error(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "alice"})
	r.handleFollow(follower, payload)

	assert.True(t, hasType(drain(follower), MsgError))
}

func TestHandleFollow_TargetNotInRoom_Error(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "ghost"})
	r.handleFollow(follower, payload)

	assert.True(t, hasType(drain(follower), MsgError))
}

func TestHandleFollow_EmptyTargetUnfollowsAndNotifiesPreviousTarget(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")
	prev := newTestClient(r, "bob", "available")
	follower.FollowTargetID = "bob"

	payload := encodePayload(ClientFollowPayload{TargetUserID: ""})
	r.handleFollow(follower, payload)

	assert.Equal(t, "", follower.FollowTargetID, "FollowTargetID should be cleared")
	assert.True(t, hasType(drain(prev), MsgFollowEnded), "previous target should receive follow_ended")
}

func TestHandleFollow_FollowPayloadContainsCorrectFollowerInfo(t *testing.T) {
	r := newTestRoom()
	follower := newTestClient(r, "alice", "available")
	follower.DisplayName = "Alice Smith"
	follower.AvatarURL = "https://cdn.example.com/alice.png"
	target := newTestClient(r, "bob", "available")

	payload := encodePayload(ClientFollowPayload{TargetUserID: "bob"})
	r.handleFollow(follower, payload)

	targetMsgs := drain(target)
	for _, m := range targetMsgs {
		if m.Type != MsgFollowStarted {
			continue
		}
		var p FollowPayload
		require.NoError(t, json.Unmarshal(m.Payload, &p))
		assert.Equal(t, "alice", p.FollowerUserID)
		assert.Equal(t, "Alice Smith", p.FollowerName)
		assert.Equal(t, "https://cdn.example.com/alice.png", p.FollowerAvatar)
		assert.True(t, p.Following)
	}
}
