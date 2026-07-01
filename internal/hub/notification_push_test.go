package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// findNotification returns the verbatim notification blob of the first
// chat:notification:new envelope, if any.
func findNotification(msgs []Envelope) (json.RawMessage, bool) {
	for _, m := range msgs {
		if m.Type != MsgChatNotificationNew {
			continue
		}
		var p ChatNotificationNewPayload
		if err := json.Unmarshal(m.Payload, &p); err == nil {
			return p.Notification, true
		}
	}
	return nil, false
}

func TestPushNotification_DeliversToOnlineUser(t *testing.T) {
	r := newTestRoom()
	h := r.hub
	h.rooms.Store(r.workspaceID, r)
	recipient := newTestClient(r, "bob", "available")

	raw := json.RawMessage(`{"id":"n1","type":"dm","is_read":false}`)
	h.PushNotification(r.workspaceID, "bob", raw)

	got, ok := findNotification(drain(recipient))
	require.True(t, ok, "online recipient should receive chat:notification:new")
	assert.JSONEq(t, string(raw), string(got), "notification blob is forwarded verbatim")
}

func TestPushNotification_NoopForUnknownTarget(t *testing.T) {
	tests := []struct {
		name        string
		workspaceID string
		userID      string
	}{
		{"unknown workspace", "other-ws", "bob"},
		{"offline user", "ws-test", "ghost"},
		{"empty workspace", "", "bob"},
		{"empty user", "ws-test", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRoom()
			h := r.hub
			h.rooms.Store(r.workspaceID, r)
			recipient := newTestClient(r, "bob", "available")

			h.PushNotification(tt.workspaceID, tt.userID, json.RawMessage(`{"id":"n1"}`))

			_, ok := findNotification(drain(recipient))
			assert.False(t, ok, "no notification should reach a connected bystander")
		})
	}
}
