package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// findChat returns the first envelope of msgType decoded into a ChatMessagePayload.
func findChat(msgs []Envelope, msgType string) (*ChatMessagePayload, bool) {
	for _, m := range msgs {
		if m.Type != msgType {
			continue
		}
		var p ChatMessagePayload
		if err := json.Unmarshal(m.Payload, &p); err == nil {
			return &p, true
		}
	}
	return nil, false
}

func joinChat(r *Room, c *Client, conversationID string) {
	r.handleChatJoin(c, encodePayload(ClientChatJoinPayload{ConversationID: conversationID}))
}

// ── chat:join / chat:leave subscription registry ──────────────────────────────

func TestHandleChatJoin_SubscribesClient(t *testing.T) {
	r := newTestRoom()
	c := newTestClient(r, "alice", "available")

	joinChat(r, c, "conv-1")

	r.chatMu.Lock()
	_, subscribed := r.chatSubs["conv-1"]["alice"]
	_, reverse := c.chatConvs["conv-1"]
	r.chatMu.Unlock()

	assert.True(t, subscribed, "client should be in conversation subscriber set")
	assert.True(t, reverse, "client reverse index should record the conversation")
}

func TestHandleChatJoin_EmptyConversationID_Error(t *testing.T) {
	r := newTestRoom()
	c := newTestClient(r, "alice", "available")

	joinChat(r, c, "")

	assert.True(t, hasType(drain(c), MsgError), "empty conversation_id should error")
	r.chatMu.Lock()
	_, hasConvs := c.chatConvs["conv-1"]
	r.chatMu.Unlock()
	assert.False(t, hasConvs)
}

func TestHandleChatLeave_UnsubscribesClient(t *testing.T) {
	r := newTestRoom()
	c := newTestClient(r, "alice", "available")
	joinChat(r, c, "conv-1")

	r.handleChatLeave(c, encodePayload(ClientChatLeavePayload{ConversationID: "conv-1"}))

	r.chatMu.Lock()
	_, stillSubbed := r.chatSubs["conv-1"]
	_, reverse := c.chatConvs["conv-1"]
	r.chatMu.Unlock()

	assert.False(t, stillSubbed, "empty conversation set should be pruned")
	assert.False(t, reverse, "reverse index should be cleared")
}

func TestHandleChatLeave_EmptyConversationID_Error(t *testing.T) {
	r := newTestRoom()
	c := newTestClient(r, "alice", "available")

	r.handleChatLeave(c, encodePayload(ClientChatLeavePayload{ConversationID: ""}))

	assert.True(t, hasType(drain(c), MsgError))
}

// ── chat:message relay (chat:message:new) ─────────────────────────────────────

func TestHandleChatMessage_RelaysToOtherSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, receiver, "conv-1")

	body := json.RawMessage(`{"id":"m1","text":"hello"}`)
	r.handleChatMessage(sender, encodePayload(ClientChatMessagePayload{
		ConversationID: "conv-1",
		Message:        body,
	}))

	p, ok := findChat(drain(receiver), MsgChatMessageNew)
	require.True(t, ok, "subscriber should receive chat:message:new")
	assert.Equal(t, "conv-1", p.ConversationID)
	assert.JSONEq(t, string(body), string(p.Message), "opaque message must be relayed verbatim")
}

func TestHandleChatMessage_DoesNotEchoToSender(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")

	r.handleChatMessage(sender, encodePayload(ClientChatMessagePayload{
		ConversationID: "conv-1",
		Message:        json.RawMessage(`{"id":"m1"}`),
	}))

	_, got := findChat(drain(sender), MsgChatMessageNew)
	assert.False(t, got, "sender must not receive its own chat:message:new")
}

func TestHandleChatMessage_NotDeliveredToNonSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	subscriber := newTestClient(r, "bob", "available")
	outsider := newTestClient(r, "carol", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, subscriber, "conv-1")
	joinChat(r, outsider, "conv-OTHER")

	r.handleChatMessage(sender, encodePayload(ClientChatMessagePayload{
		ConversationID: "conv-1",
		Message:        json.RawMessage(`{"id":"m1"}`),
	}))

	_, gotSub := findChat(drain(subscriber), MsgChatMessageNew)
	_, gotOut := findChat(drain(outsider), MsgChatMessageNew)
	assert.True(t, gotSub, "subscriber of conv-1 should receive the relay")
	assert.False(t, gotOut, "subscriber of a different conversation must not receive the relay")
}

func TestHandleChatMessage_EmptyConversationID_Error(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	r.handleChatMessage(sender, encodePayload(ClientChatMessagePayload{ConversationID: ""}))

	assert.True(t, hasType(drain(sender), MsgError))
}

// ── chat:message:edit / chat:message:delete relays ────────────────────────────

func TestHandleChatMessageEdit_RelaysToOtherSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, receiver, "conv-1")

	body := json.RawMessage(`{"id":"m1","text":"edited"}`)
	r.handleChatMessageEdit(sender, encodePayload(ClientChatMessageEditPayload{
		ConversationID: "conv-1",
		Message:        body,
	}))

	msgs := drain(receiver)
	require.True(t, hasType(msgs, MsgChatMessageEdit), "subscriber should receive chat:message:edit")
	_, senderGot := findChat(drain(sender), MsgChatMessageEdit)
	assert.False(t, senderGot, "edit must not echo to sender")
}

func TestHandleChatMessageDelete_RelaysToOtherSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, receiver, "conv-1")

	r.handleChatMessageDelete(sender, encodePayload(ClientChatMessageDeletePayload{
		ConversationID: "conv-1",
		MessageID:      "m1",
	}))

	msgs := drain(receiver)
	require.True(t, hasType(msgs, MsgChatMessageDelete), "subscriber should receive chat:message:delete")

	for _, m := range msgs {
		if m.Type != MsgChatMessageDelete {
			continue
		}
		var p ChatMessageDeletePayload
		require.NoError(t, json.Unmarshal(m.Payload, &p))
		assert.Equal(t, "conv-1", p.ConversationID)
		assert.Equal(t, "m1", p.MessageID)
	}

	assert.False(t, hasType(drain(sender), MsgChatMessageDelete), "delete must not echo to sender")
}

func TestHandleChatMessageEditDelete_EmptyConversationID_Error(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(r *Room, c *Client)
	}{
		{"edit", func(r *Room, c *Client) {
			r.handleChatMessageEdit(c, encodePayload(ClientChatMessageEditPayload{ConversationID: ""}))
		}},
		{"delete", func(r *Room, c *Client) {
			r.handleChatMessageDelete(c, encodePayload(ClientChatMessageDeletePayload{ConversationID: ""}))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRoom()
			c := newTestClient(r, "alice", "available")
			tt.invoke(r, c)
			assert.True(t, hasType(drain(c), MsgError))
		})
	}
}

// ── disconnect cleanup ─────────────────────────────────────────────────────────

func TestRemoveChatSubscriber_RemovesFromAllConversations(t *testing.T) {
	r := newTestRoom()
	c := newTestClient(r, "alice", "available")
	joinChat(r, c, "conv-1")
	joinChat(r, c, "conv-2")

	r.removeChatSubscriber(c)

	r.chatMu.Lock()
	_, c1 := r.chatSubs["conv-1"]
	_, c2 := r.chatSubs["conv-2"]
	convs := c.chatConvs
	r.chatMu.Unlock()

	assert.False(t, c1, "conv-1 set should be pruned after disconnect")
	assert.False(t, c2, "conv-2 set should be pruned after disconnect")
	assert.Nil(t, convs, "client reverse index should be cleared on disconnect")
}

func TestRelayChat_SkipsDisconnectedSubscriber(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	gone := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, gone, "conv-1")

	// bob disconnects → must be removed from the conversation, no phantom relay.
	r.removeChatSubscriber(gone)

	r.handleChatMessage(sender, encodePayload(ClientChatMessagePayload{
		ConversationID: "conv-1",
		Message:        json.RawMessage(`{"id":"m1"}`),
	}))

	_, got := findChat(drain(gone), MsgChatMessageNew)
	assert.False(t, got, "a disconnected subscriber must not receive relays")
}

// ── chat:reaction relay (chat:reaction:update) ────────────────────────────────

func TestHandleChatReaction_RelaysToOtherSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, receiver, "conv-1")

	reactions := json.RawMessage(`[{"emoji":"👍","count":2,"user_ids":["alice","bob"]}]`)
	r.handleChatReaction(sender, encodePayload(ClientChatReactionPayload{
		ConversationID: "conv-1",
		MessageID:      "m1",
		Reactions:      reactions,
	}))

	msgs := drain(receiver)
	require.True(t, hasType(msgs, MsgChatReactionUpdate), "subscriber should receive chat:reaction:update")
	for _, m := range msgs {
		if m.Type != MsgChatReactionUpdate {
			continue
		}
		var p ChatReactionUpdatePayload
		require.NoError(t, json.Unmarshal(m.Payload, &p))
		assert.Equal(t, "conv-1", p.ConversationID)
		assert.Equal(t, "m1", p.MessageID)
		assert.JSONEq(t, string(reactions), string(p.Reactions), "opaque reactions must be relayed verbatim")
	}
}

func TestHandleChatReaction_DoesNotEchoToSender(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")

	r.handleChatReaction(sender, encodePayload(ClientChatReactionPayload{
		ConversationID: "conv-1",
		MessageID:      "m1",
		Reactions:      json.RawMessage(`[]`),
	}))

	assert.False(t, hasType(drain(sender), MsgChatReactionUpdate), "reaction must not echo to sender")
}

func TestHandleChatReaction_NotDeliveredToNonSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	subscriber := newTestClient(r, "bob", "available")
	outsider := newTestClient(r, "carol", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, subscriber, "conv-1")
	joinChat(r, outsider, "conv-OTHER")

	r.handleChatReaction(sender, encodePayload(ClientChatReactionPayload{
		ConversationID: "conv-1",
		MessageID:      "m1",
		Reactions:      json.RawMessage(`[]`),
	}))

	assert.True(t, hasType(drain(subscriber), MsgChatReactionUpdate), "subscriber of conv-1 should receive the relay")
	assert.False(t, hasType(drain(outsider), MsgChatReactionUpdate), "subscriber of a different conversation must not receive the relay")
}

func TestHandleChatReaction_EmptyConversationID_Error(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	r.handleChatReaction(sender, encodePayload(ClientChatReactionPayload{ConversationID: ""}))

	assert.True(t, hasType(drain(sender), MsgError))
}

// ── chat:typing:start / chat:typing:stop relay (chat:typing) ───────────────────

func TestHandleChatTyping_StartRelaysWithTypingTrue(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, receiver, "conv-1")

	user := json.RawMessage(`{"id":"alice","name":"Alice"}`)
	r.handleChatTyping(sender, encodePayload(ClientChatTypingPayload{
		ConversationID: "conv-1",
		User:           user,
	}), true)

	msgs := drain(receiver)
	require.True(t, hasType(msgs, MsgChatTyping), "subscriber should receive chat:typing")
	for _, m := range msgs {
		if m.Type != MsgChatTyping {
			continue
		}
		var p ChatTypingPayload
		require.NoError(t, json.Unmarshal(m.Payload, &p))
		assert.Equal(t, "conv-1", p.ConversationID)
		assert.True(t, p.Typing, "typing:start must relay typing=true")
		assert.JSONEq(t, string(user), string(p.User), "opaque user must be relayed verbatim")
	}
}

func TestHandleChatTyping_StopRelaysWithTypingFalse(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, receiver, "conv-1")

	r.handleChatTyping(sender, encodePayload(ClientChatTypingPayload{
		ConversationID: "conv-1",
		User:           json.RawMessage(`{"id":"alice","name":"Alice"}`),
	}), false)

	msgs := drain(receiver)
	require.True(t, hasType(msgs, MsgChatTyping), "subscriber should receive chat:typing")
	for _, m := range msgs {
		if m.Type != MsgChatTyping {
			continue
		}
		var p ChatTypingPayload
		require.NoError(t, json.Unmarshal(m.Payload, &p))
		assert.False(t, p.Typing, "typing:stop must relay typing=false")
	}
}

func TestHandleChatTyping_DoesNotEchoToSender(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	newTestClient(r, "bob", "available")
	joinChat(r, sender, "conv-1")

	r.handleChatTyping(sender, encodePayload(ClientChatTypingPayload{
		ConversationID: "conv-1",
		User:           json.RawMessage(`{"id":"alice"}`),
	}), true)

	assert.False(t, hasType(drain(sender), MsgChatTyping), "typing must not echo to sender")
}

func TestHandleChatTyping_NotDeliveredToNonSubscribers(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	subscriber := newTestClient(r, "bob", "available")
	outsider := newTestClient(r, "carol", "available")
	joinChat(r, sender, "conv-1")
	joinChat(r, subscriber, "conv-1")
	joinChat(r, outsider, "conv-OTHER")

	r.handleChatTyping(sender, encodePayload(ClientChatTypingPayload{
		ConversationID: "conv-1",
		User:           json.RawMessage(`{"id":"alice"}`),
	}), true)

	assert.True(t, hasType(drain(subscriber), MsgChatTyping), "subscriber of conv-1 should receive the relay")
	assert.False(t, hasType(drain(outsider), MsgChatTyping), "subscriber of a different conversation must not receive the relay")
}

func TestHandleChatTyping_EmptyConversationID_Error(t *testing.T) {
	tests := []struct {
		name   string
		typing bool
	}{
		{"start", true},
		{"stop", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRoom()
			c := newTestClient(r, "alice", "available")
			r.handleChatTyping(c, encodePayload(ClientChatTypingPayload{ConversationID: ""}), tt.typing)
			assert.True(t, hasType(drain(c), MsgError))
		})
	}
}

// ── chat:conversation:notify → chat:conversation:new (per-user unicast) ────────

// findConversationNew returns the conversation_id of the first chat:conversation:new.
func findConversationNew(msgs []Envelope) (string, bool) {
	for _, m := range msgs {
		if m.Type != MsgChatConversationNew {
			continue
		}
		var p ChatConversationNewPayload
		if err := json.Unmarshal(m.Payload, &p); err == nil {
			return p.ConversationID, true
		}
	}
	return "", false
}

func TestHandleChatConversationNew_NotifiesOnlineRecipient(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	recipient := newTestClient(r, "bob", "available")

	// bob has NOT subscribed to conv-new — this is the first-contact case.
	r.handleChatConversationNew(sender, encodePayload(ClientChatConversationNotifyPayload{
		ConversationID:   "conv-new",
		RecipientUserIDs: []string{"bob"},
	}))

	convID, ok := findConversationNew(drain(recipient))
	require.True(t, ok, "online recipient should receive chat:conversation:new without subscribing")
	assert.Equal(t, "conv-new", convID)
}

func TestHandleChatConversationNew_DoesNotNotifySender(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	// Sender lists itself among recipients — it must still be skipped.
	r.handleChatConversationNew(sender, encodePayload(ClientChatConversationNotifyPayload{
		ConversationID:   "conv-new",
		RecipientUserIDs: []string{"alice"},
	}))

	_, got := findConversationNew(drain(sender))
	assert.False(t, got, "sender must not receive its own chat:conversation:new")
}

func TestHandleChatConversationNew_SkipsOfflineRecipient(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	online := newTestClient(r, "bob", "available")

	// "carol" is not connected to this room — must be skipped without affecting bob.
	r.handleChatConversationNew(sender, encodePayload(ClientChatConversationNotifyPayload{
		ConversationID:   "conv-new",
		RecipientUserIDs: []string{"carol", "bob"},
	}))

	_, ok := findConversationNew(drain(online))
	assert.True(t, ok, "an online recipient is still notified when another recipient is offline")
}

func TestHandleChatConversationNew_EmptyConversationID_Error(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")

	r.handleChatConversationNew(sender, encodePayload(ClientChatConversationNotifyPayload{
		ConversationID:   "",
		RecipientUserIDs: []string{"bob"},
	}))

	assert.True(t, hasType(drain(sender), MsgError), "empty conversation_id should error")
}

// ── dispatch via handleClientMessage (envelope wiring) ─────────────────────────

func TestHandleClientMessage_DispatchesChatEvents(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")

	joinRaw, _ := encode(ClientMsgChatJoin, ClientChatJoinPayload{ConversationID: "conv-1"})
	r.handleClientMessage(sender, joinRaw)
	r.handleClientMessage(receiver, joinRaw)

	msgRaw, _ := encode(ClientMsgChatMessage, ClientChatMessagePayload{
		ConversationID: "conv-1",
		Message:        json.RawMessage(`{"id":"m1","text":"via dispatch"}`),
	})
	r.handleClientMessage(sender, msgRaw)

	p, ok := findChat(drain(receiver), MsgChatMessageNew)
	require.True(t, ok, "chat:message routed through handleClientMessage should relay")
	assert.Equal(t, "conv-1", p.ConversationID)
}

func TestHandleClientMessage_DispatchesReactionAndTyping(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	receiver := newTestClient(r, "bob", "available")

	joinRaw, _ := encode(ClientMsgChatJoin, ClientChatJoinPayload{ConversationID: "conv-1"})
	r.handleClientMessage(sender, joinRaw)
	r.handleClientMessage(receiver, joinRaw)

	reactRaw, _ := encode(ClientMsgChatReaction, ClientChatReactionPayload{
		ConversationID: "conv-1",
		MessageID:      "m1",
		Reactions:      json.RawMessage(`[{"emoji":"🎉","count":1}]`),
	})
	r.handleClientMessage(sender, reactRaw)

	startRaw, _ := encode(ClientMsgChatTypingStart, ClientChatTypingPayload{
		ConversationID: "conv-1",
		User:           json.RawMessage(`{"id":"alice"}`),
	})
	r.handleClientMessage(sender, startRaw)

	stopRaw, _ := encode(ClientMsgChatTypingStop, ClientChatTypingPayload{
		ConversationID: "conv-1",
		User:           json.RawMessage(`{"id":"alice"}`),
	})
	r.handleClientMessage(sender, stopRaw)

	msgs := drain(receiver)
	assert.True(t, hasType(msgs, MsgChatReactionUpdate), "chat:reaction routed via dispatch should relay chat:reaction:update")
	assert.True(t, hasType(msgs, MsgChatTyping), "chat:typing:start/stop routed via dispatch should relay chat:typing")
}

func TestHandleClientMessage_DispatchesConversationNotify(t *testing.T) {
	r := newTestRoom()
	sender := newTestClient(r, "alice", "available")
	recipient := newTestClient(r, "bob", "available")

	raw, _ := encode(ClientMsgChatConversationNotify, ClientChatConversationNotifyPayload{
		ConversationID:   "conv-new",
		RecipientUserIDs: []string{"bob"},
	})
	r.handleClientMessage(sender, raw)

	convID, ok := findConversationNew(drain(recipient))
	require.True(t, ok, "chat:conversation:notify routed via handleClientMessage should unicast chat:conversation:new")
	assert.Equal(t, "conv-new", convID)
}
