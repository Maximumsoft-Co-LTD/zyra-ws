package hub

import (
	"encoding/json"
)

// Chat real-time fan-out (CHAT-006).
//
// Messages are PERSISTED via REST in zyra-api — this server never touches the DB
// and never calls zyra-api. Its only job is per-conversation relay: a client
// subscribes to conversation_ids (chat:join) and receives events only for those
// conversations. Inbound chat:message/edit/delete events are relayed to the OTHER
// subscribers of the same conversation (never echoed to the sender).
//
// Subscriptions live in Room.chatSubs (conversation_id → userID → *Client), with a
// reverse index on Client.chatConvs for O(1) disconnect cleanup. All mutations are
// serialised by Room.chatMu so the maps are never raced across goroutines.

// addChatSubscriber subscribes c to conversationID. Safe to call repeatedly.
func (r *Room) addChatSubscriber(c *Client, conversationID string) {
	r.chatMu.Lock()
	defer r.chatMu.Unlock()

	if r.chatSubs == nil {
		r.chatSubs = make(map[string]map[string]*Client)
	}
	subs := r.chatSubs[conversationID]
	if subs == nil {
		subs = make(map[string]*Client)
		r.chatSubs[conversationID] = subs
	}
	subs[c.UserID] = c

	if c.chatConvs == nil {
		c.chatConvs = make(map[string]struct{})
	}
	c.chatConvs[conversationID] = struct{}{}
}

// removeChatSubscription unsubscribes c from a single conversation.
func (r *Room) removeChatSubscription(c *Client, conversationID string) {
	r.chatMu.Lock()
	defer r.chatMu.Unlock()
	r.removeChatSubscriptionLocked(c, conversationID)
}

// removeChatSubscriptionLocked removes c from one conversation. Caller holds chatMu.
func (r *Room) removeChatSubscriptionLocked(c *Client, conversationID string) {
	if subs := r.chatSubs[conversationID]; subs != nil {
		delete(subs, c.UserID)
		if len(subs) == 0 {
			delete(r.chatSubs, conversationID)
		}
	}
	delete(c.chatConvs, conversationID)
}

// removeChatSubscriber removes c from every conversation it subscribed to. Called
// on disconnect so a departed client can never be a relay target.
func (r *Room) removeChatSubscriber(c *Client) {
	r.chatMu.Lock()
	defer r.chatMu.Unlock()
	for conversationID := range c.chatConvs {
		if subs := r.chatSubs[conversationID]; subs != nil {
			delete(subs, c.UserID)
			if len(subs) == 0 {
				delete(r.chatSubs, conversationID)
			}
		}
	}
	c.chatConvs = nil
}

// relayChat sends msg to every subscriber of conversationID except excludeUserID.
// Snapshots the subscriber set under chatMu, then sends outside the lock so a slow
// Client.Send (and its potential unregister → chatMu re-entry) can't deadlock.
func (r *Room) relayChat(conversationID, excludeUserID string, msg []byte) {
	r.chatMu.Lock()
	subs := r.chatSubs[conversationID]
	targets := make([]*Client, 0, len(subs))
	for userID, c := range subs {
		if userID != excludeUserID {
			targets = append(targets, c)
		}
	}
	r.chatMu.Unlock()

	for _, c := range targets {
		c.Send(msg)
	}
}

// handleChatConversationNew unicasts a chat:conversation:new to each named recipient
// that is currently online in this room, so a brand-new conversation (one the recipient
// has not subscribed to yet — e.g. a first-ever DM) surfaces in their sidebar live. The
// sender is never notified; offline recipients are skipped and pick the conversation up
// via REST on their next load.
func (r *Room) handleChatConversationNew(c *Client, payload json.RawMessage) {
	var p ClientChatConversationNotifyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:conversation:notify payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	msg, err := encode(MsgChatConversationNew, ChatConversationNewPayload{
		ConversationID: p.ConversationID,
	})
	if err != nil {
		return
	}
	for _, uid := range p.RecipientUserIDs {
		if uid == "" || uid == c.UserID {
			continue
		}
		if target, ok := r.getClient(uid); ok {
			target.Send(msg)
		}
	}
}

// handleChatJoin subscribes the client to a conversation's relay.
func (r *Room) handleChatJoin(c *Client, payload json.RawMessage) {
	var p ClientChatJoinPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:join payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	r.addChatSubscriber(c, p.ConversationID)
}

// handleChatLeave unsubscribes the client from a conversation's relay.
func (r *Room) handleChatLeave(c *Client, payload json.RawMessage) {
	var p ClientChatLeavePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:leave payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	r.removeChatSubscription(c, p.ConversationID)
}

// handleChatMessage relays an already-persisted message to the OTHER subscribers
// of the conversation as chat:message:new. The sender is never echoed.
func (r *Room) handleChatMessage(c *Client, payload json.RawMessage) {
	var p ClientChatMessagePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:message payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	if msg, err := encode(MsgChatMessageNew, ChatMessagePayload{
		ConversationID: p.ConversationID,
		Message:        p.Message,
	}); err == nil {
		r.relayChat(p.ConversationID, c.UserID, msg)
	}
}

// handleChatMessageEdit relays a message edit to the OTHER subscribers of the
// conversation as chat:message:edit. The sender is never echoed.
func (r *Room) handleChatMessageEdit(c *Client, payload json.RawMessage) {
	var p ClientChatMessageEditPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:message:edit payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	if msg, err := encode(MsgChatMessageEdit, ChatMessageEditPayload{
		ConversationID: p.ConversationID,
		Message:        p.Message,
	}); err == nil {
		r.relayChat(p.ConversationID, c.UserID, msg)
	}
}

// handleChatMessageDelete relays a message deletion to the OTHER subscribers of
// the conversation as chat:message:delete. The sender is never echoed.
func (r *Room) handleChatMessageDelete(c *Client, payload json.RawMessage) {
	var p ClientChatMessageDeletePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:message:delete payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	if msg, err := encode(MsgChatMessageDelete, ChatMessageDeletePayload{
		ConversationID: p.ConversationID,
		MessageID:      p.MessageID,
	}); err == nil {
		r.relayChat(p.ConversationID, c.UserID, msg)
	}
}

// handleChatReaction relays an already-persisted reaction change to the OTHER
// subscribers of the conversation as chat:reaction:update. The reactions blob is
// opaque (the client's persisted ReactionGroup[]) and forwarded verbatim. The
// sender is never echoed (CHAT-025).
func (r *Room) handleChatReaction(c *Client, payload json.RawMessage) {
	var p ClientChatReactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:reaction payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	if msg, err := encode(MsgChatReactionUpdate, ChatReactionUpdatePayload{
		ConversationID: p.ConversationID,
		MessageID:      p.MessageID,
		Reactions:      p.Reactions,
	}); err == nil {
		r.relayChat(p.ConversationID, c.UserID, msg)
	}
}

// handleChatTyping relays a typing start/stop to the OTHER subscribers of the
// conversation as chat:typing with the given typing flag. The user blob is opaque
// ({id,name}) and forwarded verbatim. The sender is never echoed (CHAT-053, WS part).
func (r *Room) handleChatTyping(c *Client, payload json.RawMessage, typing bool) {
	var p ClientChatTypingPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat:typing payload")
		return
	}
	if p.ConversationID == "" {
		r.sendError(c, "conversation_id required")
		return
	}
	if msg, err := encode(MsgChatTyping, ChatTypingPayload{
		ConversationID: p.ConversationID,
		User:           p.User,
		Typing:         typing,
	}); err == nil {
		r.relayChat(p.ConversationID, c.UserID, msg)
	}
}
