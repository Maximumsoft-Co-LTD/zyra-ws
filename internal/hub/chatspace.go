package hub

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// Chat-space sessions are the authoritative source of truth for proximity chat
// groups. Membership lives here (server-side) — clients only RENDER what the hub
// broadcasts via MsgChatSpaceState, so every viewer sees an identical set of
// circles. This replaces the old model where each client computed clusters
// independently and diverged (A/B saw green while C/D saw a different shape).
//
// Phase A scope (this file):
//   - Auto-form a session when 2+ FREE (available, not already in a session)
//     players are mutually adjacent (BFS over Chebyshev distance ≤ 1).
//   - Maintain membership as players move; drop disconnected/away members and
//     members that lose connectivity to the group.
//   - Add a member to an existing session when a join request is accepted.
//   - Broadcast the full session list whenever it changes.
//
// Lifecycle timers (100ms end / 1s grace / 5s rejoin) and the ask-to-join
// walk-to-side flow are layered on in later phases.
const (
	// chatProximityRadius is the Chebyshev tile distance within which two players
	// are considered adjacent for clustering. Matches the old client-side BFS.
	chatProximityRadius = 1
	// chatSessionMinSize is the minimum member count for a session to exist.
	chatSessionMinSize = 2
	// chatSessionTickInterval is how often the hub recomputes session membership.
	// 100ms keeps the grace timers (below) responsive without flooding CPU.
	chatSessionTickInterval = 100 * time.Millisecond

	// Lifecycle timers (Phase B):
	//   chatEndDebounce — a 2-person session ends this long after the pair separates
	//     (anti-flicker for a momentary step apart).
	//   chatLeaveGrace  — an individual member of a 3+ session is removed this long
	//     after losing connectivity to the group.
	//   chatRejoinWindow — a removed member who returns adjacent within this window
	//     rejoins the SAME session without a fresh Ask-to-Join.
	chatEndDebounce  = 100 * time.Millisecond
	chatLeaveGrace   = 1 * time.Second
	chatRejoinWindow = 5 * time.Second
)

// ChatSession is one authoritative proximity-chat group owned by the room.
type ChatSession struct {
	ID        string
	Members   []string // sorted userIDs — authoritative membership
	createdAt time.Time
}

// chatRecentLeave records a member who was removed from a still-living session,
// so a quick return (within chatRejoinWindow) rejoins it without re-asking.
type chatRecentLeave struct {
	sessionID string
	until     time.Time
}

// chatPos is a lightweight position snapshot taken under the clients range so
// the clustering math never touches live Client fields across goroutines.
type chatPos struct {
	userID string
	tx, ty int
}

// chatSpaceInit lazily allocates the session maps. Called from getOrCreateRoom
// and defensively from every mutator so a Room built without them is still safe.
func (r *Room) chatSpaceInit() {
	if r.chatSessions == nil {
		r.chatSessions = make(map[string]*ChatSession)
	}
	if r.playerSession == nil {
		r.playerSession = make(map[string]string)
	}
	if r.chatMemberLeaveAt == nil {
		r.chatMemberLeaveAt = make(map[string]time.Time)
	}
	if r.chatRecentlyLeft == nil {
		r.chatRecentlyLeft = make(map[string]chatRecentLeave)
	}
	if r.chatSuppress == nil {
		r.chatSuppress = make(map[string]map[string]struct{})
	}
}

// leaveChatSession removes a player from their session on an explicit Leave and
// suppresses auto-reforming with those same peers until the player walks away.
func (r *Room) leaveChatSession(userID string) {
	r.chatSessionMu.Lock()
	r.chatSpaceInit()
	sid, ok := r.playerSession[userID]
	if !ok {
		r.chatSessionMu.Unlock()
		return
	}
	if sess := r.chatSessions[sid]; sess != nil {
		peers := make(map[string]struct{}, len(sess.Members))
		for _, uid := range sess.Members {
			if uid != userID {
				peers[uid] = struct{}{}
			}
		}
		if len(peers) > 0 {
			r.chatSuppress[userID] = peers
		}
	}
	r.removeFromSessionLocked(sid, userID)
	delete(r.chatMemberLeaveAt, userID)
	delete(r.chatRecentlyLeft, userID) // explicit leave forfeits the rejoin window
	changed := r.chatStateChangedLocked()
	snapshot := r.chatStateSnapshotLocked()
	r.chatSessionMu.Unlock()

	if changed {
		r.broadcastChatState(snapshot)
	}
}

// runSessionTicker recomputes chat-space membership on a fixed cadence and
// broadcasts when it changes. Runs as a dedicated goroutine per Room, stopped
// by the shared stopTick channel (same as runMoveTicker).
func (r *Room) runSessionTicker() {
	t := time.NewTicker(chatSessionTickInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.recomputeChatSessions()
		case <-r.stopTick:
			return
		}
	}
}

// snapshotChatPositions returns the CURRENT tile position of every connected
// player eligible for proximity chat (status not busy/away). For a player still
// mid-walk we use their interpolated position along the path — NOT the eager
// move_to destination the server stores in TileX/TileY. Clustering on the eager
// destination would pull a player into a session the instant they START walking
// toward a group (or keep them while walking away), so the rendered pop would
// stretch across the gap to a player who hasn't actually arrived/left yet.
func (r *Room) snapshotChatPositions() map[string]chatPos {
	positions := make(map[string]chatPos)
	r.clients.Range(func(_, v any) bool {
		c := v.(*Client)
		if c.Status == "busy" || c.Status == "away" {
			return true
		}
		tx, ty := c.TileX, c.TileY
		// Capture the path header into a local so a concurrent re-path (slice
		// reassignment on the ReadPump goroutine) can't change len() mid-read.
		if path := c.MovePath; c.IsMoving && len(path) >= 2 {
			cur := currentPathTile(path, c.MoveDurationMs, c.MoveStartedAt)
			tx, ty = cur.TileX, cur.TileY
		}
		positions[c.UserID] = chatPos{userID: c.UserID, tx: tx, ty: ty}
		return true
	})
	return positions
}

// recomputeChatSessions is the heart of the session manager. It rejoins recent
// leavers, applies grace-timer maintenance to existing sessions, auto-forms new
// ones, and broadcasts the result if it changed.
func (r *Room) recomputeChatSessions() {
	positions := r.snapshotChatPositions()
	now := time.Now()

	r.chatSessionMu.Lock()
	r.chatSpaceInit()

	// 0. Rejoin pass: a member removed within the last chatRejoinWindow who is back
	//    online, still free, and adjacent to their old session rejoins it directly.
	for uid, rl := range r.chatRecentlyLeft {
		if now.After(rl.until) {
			delete(r.chatRecentlyLeft, uid)
			continue
		}
		pos, online := positions[uid]
		if !online {
			continue // wait — they may still return before the window closes
		}
		if _, inSession := r.playerSession[uid]; inSession {
			delete(r.chatRecentlyLeft, uid) // joined something else already
			continue
		}
		sess, ok := r.chatSessions[rl.sessionID]
		if !ok {
			delete(r.chatRecentlyLeft, uid) // old session is gone
			continue
		}
		if adjacentToAny(pos, sess.Members, positions) {
			sess.Members = append(sess.Members, uid)
			sort.Strings(sess.Members)
			r.playerSession[uid] = rl.sessionID
			delete(r.chatRecentlyLeft, uid)
			delete(r.chatMemberLeaveAt, uid)
		}
	}

	// 1. Maintain existing sessions with grace timers. A member that loses
	//    connectivity to the group is kept (rendered) during its grace window so the
	//    pop doesn't flicker, then removed. A 2-person session uses the short
	//    chatEndDebounce; a 3+ session uses chatLeaveGrace per departing member.
	for sid, sess := range r.chatSessions {
		keep := largestConnectedComponent(sess.Members, positions)
		connected := make(map[string]struct{}, len(keep))
		for _, uid := range keep {
			connected[uid] = struct{}{}
		}
		grace := chatLeaveGrace
		if len(sess.Members) <= 2 {
			grace = chatEndDebounce
		}
		survivors := make([]string, 0, len(sess.Members))
		removed := make([]string, 0)
		for _, uid := range sess.Members {
			if _, ok := connected[uid]; ok {
				delete(r.chatMemberLeaveAt, uid) // reconnected to the group
				survivors = append(survivors, uid)
				continue
			}
			la, leaving := r.chatMemberLeaveAt[uid]
			if !leaving {
				r.chatMemberLeaveAt[uid] = now
				survivors = append(survivors, uid) // stay during grace
				continue
			}
			if now.Sub(la) >= grace {
				removed = append(removed, uid)
				delete(r.chatMemberLeaveAt, uid)
			} else {
				survivors = append(survivors, uid)
			}
		}
		if len(survivors) < chatSessionMinSize {
			// Group collapsed — free everyone, no rejoin window (the session is gone).
			for _, uid := range sess.Members {
				delete(r.playerSession, uid)
				delete(r.chatMemberLeaveAt, uid)
				delete(r.chatRecentlyLeft, uid)
			}
			delete(r.chatSessions, sid)
			continue
		}
		// Session survives — each removed member gets a rejoin window.
		for _, uid := range removed {
			delete(r.playerSession, uid)
			r.chatRecentlyLeft[uid] = chatRecentLeave{sessionID: sid, until: now.Add(chatRejoinWindow)}
		}
		sort.Strings(survivors)
		sess.Members = survivors
	}

	// 1b. Expire Leave-suppression once the leaver is no longer adjacent to any peer
	//     they left (i.e. they walked away), so re-approaching can form a fresh session.
	for uid, peers := range r.chatSuppress {
		pos, online := positions[uid]
		stillNear := false
		if online {
			for peer := range peers {
				if pp, ok := positions[peer]; ok && adjacent(pos, pp) {
					stillNear = true
					break
				}
			}
		}
		if !stillNear {
			delete(r.chatSuppress, uid)
		}
	}

	// 2. Auto-form: cluster FREE players (eligible and not already in a session),
	//    honouring Leave suppression so a leaver doesn't instantly re-cluster.
	free := make([]chatPos, 0, len(positions))
	for uid, p := range positions {
		if _, inSession := r.playerSession[uid]; !inSession {
			free = append(free, p)
		}
	}
	blocked := func(a, b string) bool {
		if peers, ok := r.chatSuppress[a]; ok {
			if _, hit := peers[b]; hit {
				return true
			}
		}
		if peers, ok := r.chatSuppress[b]; ok {
			if _, hit := peers[a]; hit {
				return true
			}
		}
		return false
	}
	for _, cluster := range bfsClusters(free, blocked) {
		if len(cluster) < chatSessionMinSize {
			continue
		}
		members := make([]string, 0, len(cluster))
		for _, p := range cluster {
			members = append(members, p.userID)
		}
		sort.Strings(members)
		sid := "cs_" + generateRequestID()
		r.chatSessions[sid] = &ChatSession{ID: sid, Members: members, createdAt: time.Now()}
		for _, uid := range members {
			r.playerSession[uid] = sid
		}
	}

	changed := r.chatStateChangedLocked()
	snapshot := r.chatStateSnapshotLocked()
	r.chatSessionMu.Unlock()

	if changed {
		r.broadcastChatState(snapshot)
	}
}

// addToChatSession adds a member to an existing session (on accepted join) and
// broadcasts the new state immediately so the accept feels instant. No-op if the
// session no longer exists. Returns true if the member was actually added.
func (r *Room) addToChatSession(sessionID, userID string) bool {
	r.chatSessionMu.Lock()
	r.chatSpaceInit()
	sess, ok := r.chatSessions[sessionID]
	if !ok {
		r.chatSessionMu.Unlock()
		return false
	}
	for _, uid := range sess.Members {
		if uid == userID {
			r.chatSessionMu.Unlock()
			return false // already a member
		}
	}
	// If the joiner was in another session, remove them from it first.
	if prev, inPrev := r.playerSession[userID]; inPrev && prev != sessionID {
		r.removeFromSessionLocked(prev, userID)
	}
	sess.Members = append(sess.Members, userID)
	sort.Strings(sess.Members)
	r.playerSession[userID] = sessionID
	snapshot := r.chatStateSnapshotLocked()
	r.chatStateHash = chatStateHash(snapshot)
	r.chatSessionMu.Unlock()

	r.broadcastChatState(snapshot)
	return true
}

// removeFromChatSession drops a member from whatever session they're in (e.g. on
// disconnect) and broadcasts if the membership actually changed.
func (r *Room) removeFromChatSession(userID string) {
	r.chatSessionMu.Lock()
	r.chatSpaceInit()
	sid, ok := r.playerSession[userID]
	if !ok {
		r.chatSessionMu.Unlock()
		return
	}
	r.removeFromSessionLocked(sid, userID)
	changed := r.chatStateChangedLocked()
	snapshot := r.chatStateSnapshotLocked()
	r.chatSessionMu.Unlock()

	if changed {
		r.broadcastChatState(snapshot)
	}
}

// removeFromSessionLocked removes userID from session sid, dissolving the session
// if it drops below the minimum size. Caller must hold chatSessionMu.
func (r *Room) removeFromSessionLocked(sid, userID string) {
	sess, ok := r.chatSessions[sid]
	if !ok {
		delete(r.playerSession, userID)
		return
	}
	kept := sess.Members[:0:0]
	for _, uid := range sess.Members {
		if uid != userID {
			kept = append(kept, uid)
		}
	}
	delete(r.playerSession, userID)
	if len(kept) < chatSessionMinSize {
		for _, uid := range kept {
			delete(r.playerSession, uid)
		}
		delete(r.chatSessions, sid)
		return
	}
	sess.Members = kept
}

// --- change detection + broadcast ----------------------------------------

// chatStateSnapshotLocked builds a deterministic, sorted DTO list. Caller holds
// chatSessionMu.
func (r *Room) chatStateSnapshotLocked() []ChatSpaceSessionDTO {
	out := make([]ChatSpaceSessionDTO, 0, len(r.chatSessions))
	for _, sess := range r.chatSessions {
		members := make([]string, len(sess.Members))
		copy(members, sess.Members)
		sort.Strings(members)
		out = append(out, ChatSpaceSessionDTO{ID: sess.ID, MemberIDs: members})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// chatStateChangedLocked compares the current state hash against the last
// broadcast hash, updating it when different. Caller holds chatSessionMu.
func (r *Room) chatStateChangedLocked() bool {
	h := chatStateHash(r.chatStateSnapshotLocked())
	if h == r.chatStateHash {
		return false
	}
	r.chatStateHash = h
	return true
}

// chatStateHash produces a stable string fingerprint of the session list.
func chatStateHash(sessions []ChatSpaceSessionDTO) string {
	var b strings.Builder
	for _, s := range sessions {
		b.WriteString(s.ID)
		b.WriteByte('=')
		b.WriteString(strings.Join(s.MemberIDs, ","))
		b.WriteByte(';')
	}
	return b.String()
}

// sendChatStateTo unicasts the current session list to a single client (used on
// join, since the ticker only broadcasts on change). Always sends — even an empty
// list — so a reconnecting client that left a session has its stale pops cleared.
func (r *Room) sendChatStateTo(c *Client) {
	r.chatSessionMu.Lock()
	snapshot := r.chatStateSnapshotLocked()
	r.chatSessionMu.Unlock()
	if msg, err := encode(MsgChatSpaceState, ChatSpaceStatePayload{Sessions: snapshot}); err == nil {
		c.Send(msg)
	}
}

// broadcastChatState encodes and broadcasts the authoritative session list to
// every client in the room, and best-effort persists it to Redis.
func (r *Room) broadcastChatState(sessions []ChatSpaceSessionDTO) {
	if msg, err := encode(MsgChatSpaceState, ChatSpaceStatePayload{Sessions: sessions}); err == nil {
		r.broadcast(msg)
	}
	if s := r.redisStore(); s != nil {
		if raw, err := json.Marshal(sessions); err == nil {
			_ = s.SaveChatSessions(context.Background(), r.workspaceID, raw)
		}
	}
}

// --- clustering helpers ----------------------------------------------------

// bfsClusters groups players into connected components where an edge exists
// between two players within Chebyshev distance chatProximityRadius. When blocked
// is non-nil, an edge is skipped if blocked(a,b) — used to honour Leave suppression
// so an explicit leaver doesn't instantly re-cluster with the peers they left.
func bfsClusters(players []chatPos, blocked func(a, b string) bool) [][]chatPos {
	visited := make(map[string]bool, len(players))
	var clusters [][]chatPos
	for _, seed := range players {
		if visited[seed.userID] {
			continue
		}
		var cluster []chatPos
		queue := []chatPos{seed}
		visited[seed.userID] = true
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			cluster = append(cluster, cur)
			for _, other := range players {
				if visited[other.userID] {
					continue
				}
				if adjacent(cur, other) && (blocked == nil || !blocked(cur.userID, other.userID)) {
					visited[other.userID] = true
					queue = append(queue, other)
				}
			}
		}
		clusters = append(clusters, cluster)
	}
	return clusters
}

// largestConnectedComponent returns the biggest group of the given member ids
// that remain mutually connected (transitively, Chebyshev ≤ radius). Members not
// present in positions are ignored. Ties broken by the lowest-sorted member id so
// the result is deterministic.
func largestConnectedComponent(memberIDs []string, positions map[string]chatPos) []string {
	pts := make([]chatPos, 0, len(memberIDs))
	for _, uid := range memberIDs {
		if p, ok := positions[uid]; ok {
			pts = append(pts, p)
		}
	}
	if len(pts) == 0 {
		return nil
	}
	var best []chatPos
	for _, comp := range bfsClusters(pts, nil) {
		if len(comp) > len(best) {
			best = comp
		} else if len(comp) == len(best) && len(comp) > 0 {
			if minUserID(comp) < minUserID(best) {
				best = comp
			}
		}
	}
	out := make([]string, 0, len(best))
	for _, p := range best {
		out = append(out, p.userID)
	}
	sort.Strings(out)
	return out
}

// adjacentToAny reports whether pos is within chatProximityRadius of any of the
// given members that currently have a known position.
func adjacentToAny(pos chatPos, members []string, positions map[string]chatPos) bool {
	for _, uid := range members {
		if mp, ok := positions[uid]; ok && adjacent(pos, mp) {
			return true
		}
	}
	return false
}

func adjacent(a, b chatPos) bool {
	dx := a.tx - b.tx
	if dx < 0 {
		dx = -dx
	}
	dy := a.ty - b.ty
	if dy < 0 {
		dy = -dy
	}
	if dx > dy {
		return dx <= chatProximityRadius
	}
	return dy <= chatProximityRadius
}

func minUserID(pts []chatPos) string {
	m := pts[0].userID
	for _, p := range pts[1:] {
		if p.userID < m {
			m = p.userID
		}
	}
	return m
}
