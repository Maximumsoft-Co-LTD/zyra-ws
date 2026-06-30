package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"

	"zyra-ws/internal/store"
)

const maxChatLength = 500

// knockEntry holds pending knock-request state.
type knockEntry struct {
	requesterID string
	zoneID      string
}

// denyTracker tracks consecutive knock denials per zone+user (in-memory fallback when Redis is unavailable).
type denyTracker struct {
	count   int64
	expires time.Time
}

const denyTrackerTTL = 10 * time.Minute

// pendingMoveEntry holds the latest buffered move for one player between tick flushes.
// roomID is captured at buffer time so flushMoves can route without touching the Client.
type pendingMoveEntry struct {
	moved  MovedPayload
	roomID string
}

// Room represents all connected clients within a single workspace.
type Room struct {
	workspaceID   string
	clients       sync.Map // userID → *Client
	hub           *Hub
	waveCooldowns sync.Map // "senderID:targetID" → time.Time (expiry) — fallback when Redis unavailable
	knockPending  sync.Map // requestID → knockEntry
	knockDenies   sync.Map // "zoneID:userID" → *denyTracker — in-memory deny count fallback
	aoi           *AOIGrid // open-floor spatial index for move broadcast filtering
	pendingMoves  sync.Map // userID → pendingMoveEntry — latest position per player, flushed every 50ms
	stopTick      chan struct{}
	stopTickOnce  sync.Once
	// followMu serialises follow-chain mutations (handleFollow/detach) and the
	// server-driven cascade that walks hidden followers, so chain links and the
	// followers' movement state are never mutated by two goroutines at once.
	followMu sync.Mutex

	// lifecycleMu serialises the register/unregister critical section so two
	// simultaneous joins (or a join coinciding with a leave) can't interleave
	// between the peer-snapshot and the joined/left broadcast. Without it, two
	// clients connecting at the same instant can each miss the other's presence:
	// one sees both players, the other sees only itself (asymmetric presence).
	lifecycleMu sync.Mutex

	// Obstacle grid for movement validation (bounds + blocked tiles). Loaded from
	// Redis and refreshed every obstacleTTL so an owner's mid-session grid upload
	// propagates without recreating the room. nil = no grid → validation disabled.
	obstacleMu       sync.Mutex
	obstacleLoadedAt time.Time
	obstacleGrid     *store.ObstacleGrid

	// Chat real-time fan-out registry (CHAT-006). Maps conversation_id → set of
	// subscribed clients (userID → *Client) so a chat event relays only to the
	// members of that conversation, not the whole workspace. Guarded by chatMu;
	// the reverse set lives on each Client (Client.chatConvs) for O(1) disconnect
	// cleanup. Messages are persisted via REST in zyra-api — this is relay only.
	chatMu   sync.Mutex
	chatSubs map[string]map[string]*Client // conversation_id → userID → client
}

// obstacleTTL bounds how stale the cached obstacle grid may be.
const obstacleTTL = 30 * time.Second

// redisStore returns the store from the hub (may be nil).
func (r *Room) redisStore() *store.RedisStore {
	return r.hub.store
}

// incrementDenyCount increments the in-memory deny counter for zone+user and returns the new total.
func (r *Room) incrementDenyCount(zoneID, userID string) int64 {
	key := zoneID + ":" + userID
	now := time.Now()
	v, loaded := r.knockDenies.Load(key)
	if loaded {
		t := v.(*denyTracker)
		if now.After(t.expires) {
			t.count = 1
		} else {
			t.count++
		}
		t.expires = now.Add(denyTrackerTTL)
		return t.count
	}
	t := &denyTracker{count: 1, expires: now.Add(denyTrackerTTL)}
	r.knockDenies.Store(key, t)
	return 1
}

// register adds a client to the room and broadcasts a joined event to others.
// It also sends a welcome message directly to the new client with the full room state.
func (r *Room) register(c *Client) {
	// Capture any existing connection before overwriting, so we can close it
	// AFTER storing the new client. The close must happen after Store so that
	// the old goroutine's unregister call hits the CompareAndDelete guard and
	// returns early, preventing a phantom "left" broadcast.
	// Serialise the whole register sequence (peer snapshot → store → joined
	// broadcast) against concurrent register/unregister so a simultaneous join
	// can never lose its joined broadcast or be absent from the peer's welcome
	// snapshot. Held across the welcome/follow restore too so the trailing
	// joined broadcast still carries the restored FollowTargetID.
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	var oldClient *Client
	if old, ok := r.clients.Load(c.UserID); ok {
		oldClient = old.(*Client)
	}

	// Build current player list before adding the new client.
	others := r.players(c.UserID)

	// Store the new client first, then evict the old one.
	r.clients.Store(c.UserID, c)
	// Seed initial position into the AOI grid so the client is visible to
	// nearby peers immediately — even before they send their first move.
	r.aoi.Move(c, c.TileX, c.TileY)

	if oldClient != nil {
		// Notify the old tab/device that it was superseded, then close after a
		// short delay so the WritePump has time to flush the message.
		if msg, err := encode(MsgSessionReplaced, SessionReplacedPayload{
			Reason: "Another session connected to this workspace",
		}); err == nil {
			oldClient.Send(msg)
		}
		go func(oc *Client) {
			time.Sleep(200 * time.Millisecond)
			_ = oc.conn.Close()
		}(oldClient)
	}

	slog.Info("ws room join", "workspace_id", r.workspaceID, "user_id", c.UserID, "online", r.count())

	ctx := context.Background()

	// Build pending knocks for this user (requester side — restores "waiting" overlay).
	var pendingKnocks []PendingKnock
	// Build active knock requests for this workspace (occupant side — restores knock notifications).
	var activeKnockRequests []ActiveKnockRequest
	if s := r.redisStore(); s != nil {
		if entries, err := s.GetPendingKnocks(ctx, r.workspaceID, c.UserID); err == nil {
			for _, e := range entries {
				pendingKnocks = append(pendingKnocks, PendingKnock{RequestID: e.RequestID, ZoneID: e.ZoneID})
			}
		}
		if requests, err := s.GetWorkspaceKnockRequests(ctx, r.workspaceID); err == nil {
			for _, req := range requests {
				// Skip knocks initiated by this reconnecting user (they use pending_knocks instead).
				if req.RequesterUserID == c.UserID {
					continue
				}
				activeKnockRequests = append(activeKnockRequests, ActiveKnockRequest{
					RequestID:       req.RequestID,
					ZoneID:          req.ZoneID,
					RequesterUserID: req.RequesterUserID,
					RequesterName:   req.RequesterName,
					RequesterAvatar: req.RequesterAvatar,
				})
			}
		}
	}

	// Restore follow chain from Redis if the user reconnected quickly enough.
	var followTargetID string
	if s := r.redisStore(); s != nil {
		if savedTarget, err := s.GetFollow(ctx, r.workspaceID, c.UserID); err == nil && savedTarget != "" {
			if target, ok := r.getClient(savedTarget); ok {
				r.followMu.Lock()
				tail := r.resolveTail(target)
				if tail.UserID != c.UserID {
					c.FollowTargetID = tail.UserID
					tail.FollowerID = c.UserID
					r.persistFollow(c, tail.UserID)
					followTargetID = tail.UserID
					r.sendMsg(tail, MsgFollowStarted, followPayload(c, true))
				}
				r.followMu.Unlock()
			}
			// Clean up the reconnect key now that we consumed it.
			_ = s.DeleteFollow(ctx, r.workspaceID, c.UserID)
		}
	}

	// Send welcome to the new client with full state snapshot.
	welcome := WelcomePayload{
		Me:                  c.Player(),
		Players:             others,
		PendingKnocks:       pendingKnocks,
		ActiveKnockRequests: activeKnockRequests,
		FollowTargetID:      followTargetID,
	}
	if msg, err := encode(MsgWelcome, welcome); err == nil {
		c.Send(msg)
	}

	// If the follow chain was restored, tell the reconnected client which node
	// to walk behind (same as handleFollow does for new follows).
	if followTargetID != "" {
		r.followMu.Lock()
		leader := r.resolveLeader(c)
		r.followMu.Unlock()
		r.sendMsg(c, MsgFollowAssigned, FollowAssignedPayload{
			TargetUserID: followTargetID,
			ChainLeader:  leader.UserID,
		})
	}

	// Broadcast joined event to all others.
	if msg, err := encode(MsgJoined, JoinedPayload{Player: c.Player()}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

// unregister removes a client and broadcasts a left event.
func (r *Room) unregister(c *Client) {
	// Serialise with register so the delete + left broadcast can't interleave
	// between a concurrent join's peer-snapshot and its joined broadcast.
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	// Guard: only unregister if this exact client instance is still registered.
	// This prevents a stale goroutine (from a previous connection of the same user)
	// from evicting a newer reconnected client and emitting a phantom "left" event.
	// Scenario: React StrictMode double-mount or brief network reconnect can leave
	// an old goroutine alive after a new connection has already replaced it in the map.
	if !r.clients.CompareAndDelete(c.UserID, c) {
		return
	}

	slog.Info("ws room leave", "workspace_id", r.workspaceID, "user_id", c.UserID, "online", r.count())

	// Heal the follow chain: re-link this client's neighbours so a mid-chain
	// disconnect doesn't strand the trailing followers (Follower 2 leaves ->
	// Follower 3 retargets Follower 1). Runs before AOI/Redis cleanup; c is already
	// out of the clients map but its neighbours are still connected.
	r.followMu.Lock()
	r.detach(c)
	r.followMu.Unlock()

	// Remove from AOI grid so departing player stops receiving broadcasts.
	r.aoi.Remove(c.UserID)

	// Drop the client from every chat conversation it subscribed to so no relay
	// targets a disconnected connection (CHAT-006).
	r.removeChatSubscriber(c)

	// Remove Redis presence, position snapshot, and save last tile (LP-3).
	// If the player was mid-walk (IsMoving), save the pre-move start tile
	// instead of the destination tile — the player never actually reached it.
	if s := r.redisStore(); s != nil {
		ctx := context.Background()
		saveTX, saveTY := c.TileX, c.TileY
		if c.IsMoving {
			saveTX, saveTY = c.MoveStartTX, c.MoveStartTY
		}
		if saveTX != 0 || saveTY != 0 {
			_ = s.SetLastPosition(ctx, r.workspaceID, c.UserID, saveTX, saveTY)
		}
		_ = s.DeletePresence(ctx, r.workspaceID, c.UserID)
		_ = s.ClearRoom(ctx, r.workspaceID, c.UserID)
		// Keep the follow target in Redis briefly so a quick reconnect can
		// restore the chain instead of forcing the user to re-follow.
		_ = s.ExpireFollow(ctx, r.workspaceID, c.UserID)
		_ = s.DeletePosSnapshot(ctx, r.workspaceID, c.UserID)
	}

	if msg, err := encode(MsgLeft, LeftPayload{UserID: c.UserID}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}

	if r.count() == 0 {
		r.hub.removeRoom(r.workspaceID)
		r.shutdown()
	}
}

// handleClientMessage routes an inbound JSON message from a client.
func (r *Room) handleClientMessage(c *Client, raw []byte) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		r.sendError(c, "invalid message format")
		return
	}

	switch env.Type {
	case ClientMsgMove:
		r.handleMove(c, env.Payload)
	case ClientMsgChat:
		r.handleChat(c, env.Payload)
	case ClientMsgPing:
		r.handlePing(c, env.Payload)
	case ClientMsgStatus:
		r.handleStatus(c, env.Payload)
	case ClientMsgRoomEnter:
		r.handleRoomEnter(c, env.Payload)
	case ClientMsgRoomExit:
		r.handleRoomExit(c, env.Payload)
	case ClientMsgWave:
		r.handleWave(c, env.Payload)
	case ClientMsgFollow:
		r.handleFollow(c, env.Payload)
	case ClientMsgStopFollower:
		r.handleStopFollower(c, env.Payload)
	case ClientMsgKnock:
		r.handleKnock(c, env.Payload)
	case ClientMsgKnockDecide:
		r.handleKnockDecide(c, env.Payload)
	case ClientMsgKnockCancel:
		r.handleKnockCancel(c, env.Payload)
	case ClientMsgMoveTo:
		r.handleMoveTo(c, env.Payload)
	case ClientMsgStop:
		r.handleStop(c, env.Payload)
	case ClientMsgHeartbeat:
		r.handleHeartbeat(c)
	case ClientMsgSectionSync:
		r.handleSectionSync(c, env.Payload)
	case ClientMsgVisibility:
		r.handleVisibility(c, env.Payload)
	case ClientMsgChatJoin:
		r.handleChatJoin(c, env.Payload)
	case ClientMsgChatLeave:
		r.handleChatLeave(c, env.Payload)
	case ClientMsgChatMessage:
		r.handleChatMessage(c, env.Payload)
	case ClientMsgChatMessageEdit:
		r.handleChatMessageEdit(c, env.Payload)
	case ClientMsgChatMessageDelete:
		r.handleChatMessageDelete(c, env.Payload)
	case ClientMsgChatReaction:
		r.handleChatReaction(c, env.Payload)
	case ClientMsgChatTypingStart:
		r.handleChatTyping(c, env.Payload, true)
	case ClientMsgChatTypingStop:
		r.handleChatTyping(c, env.Payload, false)
	default:
		r.sendError(c, "unknown message type: "+env.Type)
	}
}

// absInt returns the absolute value of n.
func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func (r *Room) handleMove(c *Client, payload json.RawMessage) {
	if c.Hidden && c.FollowTargetID != "" {
		return // server-driven while hidden
	}
	var p ClientMovePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid move payload")
		return
	}

	// VO05-2: Teleport detection — reject moves that jump more than 3 tiles at once.
	// Only enforced once the player has a known position (non-zero initial tile).
	dx := absInt(p.TileX - c.TileX)
	dy := absInt(p.TileY - c.TileY)
	if (dx > 3 || dy > 3) && (c.TileX != 0 || c.TileY != 0) {
		// Reject and snap the client back — server position is unchanged here,
		// so forceSync reflects the last accepted tile.
		r.forceSync(c, "teleport")
		return
	}

	// Out-of-bounds / blocked-tile guard — server position is unchanged on reject.
	if r.tileBlocked(r.getObstacleGrid(), p.TileX, p.TileY) {
		r.forceSync(c, "blocked_dest")
		return
	}

	// Update in-place — no need to re-store because the pointer is shared.
	c.TileX = p.TileX
	c.TileY = p.TileY
	if p.PX != 0 || p.PY != 0 {
		c.PX = p.PX
		c.PY = p.PY
	}
	prevAvatarURL := c.AvatarURL
	if p.AvatarURL != "" {
		c.AvatarURL = p.AvatarURL
	}
	if p.Direction != "" {
		c.Direction = p.Direction
	}
	c.Sitting = p.Sitting

	moved := MovedPayload{
		UserID:    c.UserID,
		TileX:     c.TileX,
		TileY:     c.TileY,
		PX:        c.PX,
		PY:        c.PY,
		AvatarURL: c.AvatarURL,
		Direction: c.Direction,
		Sitting:   c.Sitting,
	}

	// When AvatarURL changes (e.g. player transitions between sitting and standing
	// spritesheets), immediately broadcast a full JSON moved event so peers can
	// update their texture cache.  Regular binary tick frames omit avatar_url for
	// bandwidth efficiency, so peers would otherwise never learn the new URL.
	if prevAvatarURL != c.AvatarURL {
		if msg, err := encode(MsgMoved, moved); err == nil {
			r.broadcastExcept(msg, c.UserID)
		}
	}
	if s := r.redisStore(); s != nil {
		// Throttle snapshot writes to 1/s — new joiners only need periodic accuracy,
		// not a Redis write on every 20 Hz move (was 4 cmds/move = 80k cmds/s at 1k CCU).
		if time.Since(c.lastSnapAt) >= time.Second {
			c.lastSnapAt = time.Now()
			ctx := context.Background()
			if raw, err := json.Marshal(moved); err == nil {
				_ = s.SavePosSnapshot(ctx, r.workspaceID, c.UserID, raw)
			}
		}
	}

	// Update the spatial grid and buffer this move for the next tick flush.
	// Encoding and fan-out happen in flushMoves every 20ms — this keeps the
	// hot path (ReadPump goroutine) free of encode + per-peer Send overhead.
	r.aoi.Move(c, c.TileX, c.TileY)
	r.pendingMoves.Store(c.UserID, pendingMoveEntry{moved: moved, roomID: c.RoomID})
}

// currentPathTile returns the tile a player is at right now along an active path,
// based on how long ago the walk started. Steps are one tile each (equidistant),
// so elapsed time maps linearly to the waypoint index. Used to reconcile the
// server's authoritative position when a player re-paths before finishing the
// previous path (see handleMoveTo).
func currentPathTile(path []TilePoint, durationMs int, startedAt time.Time) TilePoint {
	n := len(path)
	if n == 0 {
		return TilePoint{}
	}
	if durationMs <= 0 || n == 1 {
		return path[n-1]
	}
	elapsed := int(time.Since(startedAt).Milliseconds())
	if elapsed <= 0 {
		return path[0]
	}
	if elapsed >= durationMs {
		return path[n-1]
	}
	// idx = round(progress * (n-1)) using integer math (no math.Round import).
	idx := (elapsed*(n-1) + durationMs/2) / durationMs
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return path[idx]
}

// handleMoveTo processes a path-based movement request.
// The client sends a path of tile waypoints; the server calculates the travel
// duration from the path distance and player speed, then broadcasts a "moving"
// event to all peers so they can interpolate the position locally.
func (r *Room) handleMoveTo(c *Client, payload json.RawMessage) {
	// A hidden follower is walked by the server (cascadeHiddenFollowers); ignore any
	// stale self-move it may emit so the two drivers never fight over its position.
	if c.Hidden && c.FollowTargetID != "" {
		return
	}
	var p ClientMoveToPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid move_to payload")
		return
	}
	if len(p.Path) < 2 {
		r.sendError(c, "move_to path must have at least 2 points")
		return
	}
	if len(p.Path) > maxPathLen {
		r.sendError(c, fmt.Sprintf("move_to path too long (max %d)", maxPathLen))
		return
	}

	// Validate that consecutive tiles are adjacent (Manhattan distance ≤ 1 per step).
	for i := 1; i < len(p.Path); i++ {
		dx := absInt(p.Path[i].TileX - p.Path[i-1].TileX)
		dy := absInt(p.Path[i].TileY - p.Path[i-1].TileY)
		if dx > 1 || dy > 1 || (dx == 0 && dy == 0) {
			// Malformed path — snap the client back to the last accepted position.
			r.forceSync(c, "invalid_path")
			return
		}
	}

	// If the player is still mid-path from a previous move_to, advance the server's
	// authoritative position to where they actually are NOW before validating the
	// new path's start. On receipt the server snaps TileX/TileY to a move_to's
	// DESTINATION (below), assuming the whole path is walked. A follower re-paths
	// continuously as its target moves, so its next move_to starts from its real
	// mid-path tile — not that destination. Checking that against the stale
	// destination wrongly trips desync_start → force_sync, snapping the follower's
	// own avatar (the "walk down then warp back" the moving player sees). Reconcile.
	if c.IsMoving && len(c.MovePath) >= 2 {
		cur := currentPathTile(c.MovePath, c.MoveDurationMs, c.MoveStartedAt)
		c.TileX = cur.TileX
		c.TileY = cur.TileY
		c.PX, c.PY = tileCenterPx(cur.TileX, cur.TileY)
	}

	// Validate start position is close to the player's current position.
	start := p.Path[0]
	if (c.TileX != 0 || c.TileY != 0) &&
		(absInt(start.TileX-c.TileX) > 3 || absInt(start.TileY-c.TileY) > 3) {
		// Client's path start drifted from the server position — reconcile.
		r.forceSync(c, "desync_start")
		return
	}

	// Wall/bounds validation: check destination + wall-edge crossings.
	grid := r.getObstacleGrid()
	dest := p.Path[len(p.Path)-1]
	if r.tileBlocked(grid, dest.TileX, dest.TileY) {
		r.forceSync(c, "blocked_dest")
		return
	}
	if r.pathCrossesWall(grid, p.Path) {
		r.forceSync(c, "wall_crossing")
		return
	}

	if p.AvatarURL != "" {
		c.AvatarURL = p.AvatarURL
	}

	// A moving player is, by definition, standing. Clear the sitting flag so the
	// welcome/joined snapshot (Player()) never reports a player as sitting while
	// they walk — otherwise newly joined / resynced peers render the character in
	// a seated pose floating along the path.
	c.Sitting = false

	// Capture pre-move position once (first unconfirmed walk). On disconnect
	// before stop arrives, unregister saves this tile instead of the unreached
	// destination, preventing players from teleporting forward after reconnect.
	if !c.IsMoving {
		c.MoveStartTX = c.TileX
		c.MoveStartTY = c.TileY
	}
	c.IsMoving = true

	// Update client state to the destination tile for AOI and snapshot.
	c.TileX = dest.TileX
	c.TileY = dest.TileY
	c.PX, c.PY = tileCenterPx(dest.TileX, dest.TileY)

	// Honour the client's actual walk speed (sprint), but clamp it to the allowed
	// range so a peer can't claim an impossible speed. This keeps every client's
	// interpolation in step with the speed the mover walks locally — a sprinting
	// player no longer drifts ahead on their own screen while peers render them slow.
	speed := clampSpeed(p.Speed)
	durationMs := pathDurationMs(p.Path, speed)

	// Store active path so newly joined clients can resume interpolation.
	c.MovePath = p.Path
	c.MoveDurationMs = durationMs
	c.MoveSpeed = speed
	c.MoveStartedAt = time.Now()
	if durationMs == 0 {
		return
	}

	moving := MovingPayload{
		UserID:       c.UserID,
		Path:         p.Path,
		DurationMs:   durationMs,
		Speed:        speed,
		AvatarURL:    c.AvatarURL,
		ServerTimeMs: c.MoveStartedAt.UnixMilli(),
	}

	if msg, err := encode(MsgMoving, moving); err == nil {
		if c.RoomID != "" {
			r.broadcastToRoom(msg, c.UserID, c.RoomID)
		} else {
			r.broadcastExcept(msg, c.UserID)
		}
	}

	r.aoi.Move(c, c.TileX, c.TileY)

	// Drive any backgrounded followers behind this mover one tile behind, mirroring
	// this path — keeps the chain moving when a middle node is hidden.
	slog.Info("[move_to] cascade check",
		"mover", c.UserID,
		"followerID", c.FollowerID,
		"moverHidden", c.Hidden,
	)
	r.cascadeHiddenFollowers(c, p.Path)

	// Throttled Redis snapshot for reconnect/new-joiner state.
	if s := r.redisStore(); s != nil && time.Since(c.lastSnapAt) >= time.Second {
		c.lastSnapAt = time.Now()
		ctx := context.Background()
		moved := MovedPayload{
			UserID: c.UserID, TileX: c.TileX, TileY: c.TileY,
			PX: c.PX, PY: c.PY, AvatarURL: c.AvatarURL,
		}
		if raw, err := json.Marshal(moved); err == nil {
			_ = s.SavePosSnapshot(ctx, r.workspaceID, c.UserID, raw)
		}
	}
}

// handleStop processes a movement interruption.
// The client sends their current position; the server broadcasts a "stopped"
// event so peers can halt the ongoing path interpolation at the correct spot.
func (r *Room) handleStop(c *Client, payload json.RawMessage) {
	if c.Hidden && c.FollowTargetID != "" {
		return // server-driven while hidden
	}
	var p ClientStopPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid stop payload")
		return
	}

	c.IsMoving = false
	c.MovePath = nil
	c.TileX = p.TileX
	c.TileY = p.TileY
	if p.PX != 0 || p.PY != 0 {
		c.PX = p.PX
		c.PY = p.PY
	}
	// Adopt the final facing direction from the stop payload. moveTimer is
	// suppressed during path walks, so c.Direction would otherwise be stale
	// and peers would render the wrong facing after the walk ends.
	if p.Direction != "" {
		c.Direction = p.Direction
	}
	c.Sitting = p.Sitting

	stopped := StoppedPayload{
		UserID:    c.UserID,
		TileX:     c.TileX,
		TileY:     c.TileY,
		PX:        c.PX,
		PY:        c.PY,
		Direction: c.Direction,
		Sitting:   c.Sitting,
	}

	if msg, err := encode(MsgStopped, stopped); err == nil {
		if c.RoomID != "" {
			r.broadcastToRoom(msg, c.UserID, c.RoomID)
		} else {
			r.broadcastExcept(msg, c.UserID)
		}
	}

	r.aoi.Move(c, c.TileX, c.TileY)

	// Cascade stop to hidden followers so they halt at their current position
	// instead of continuing a stale server-driven walk.
	r.cascadeHiddenFollowersStop(c)
}

// cascadeHiddenFollowersStop broadcasts a "stopped" for each hidden follower
// behind the mover so peers halt their interpolation. Without this, a hidden
// follower's last server-driven "moving" would play out to its full duration
// while the mover has already stopped.
func (r *Room) cascadeHiddenFollowersStop(mover *Client) {
	r.followMu.Lock()
	defer r.followMu.Unlock()
	node := mover
	for i := 0; i < maxFollowChain; i++ {
		if node.FollowerID == "" {
			return
		}
		f, ok := r.getClient(node.FollowerID)
		if !ok || !f.Hidden {
			return
		}
		if f.IsMoving && len(f.MovePath) >= 2 {
			cur := currentPathTile(f.MovePath, f.MoveDurationMs, f.MoveStartedAt)
			f.TileX = cur.TileX
			f.TileY = cur.TileY
			f.PX, f.PY = tileCenterPx(cur.TileX, cur.TileY)
		}
		f.IsMoving = false
		f.MovePath = nil
		if msg, err := encode(MsgStopped, StoppedPayload{
			UserID:    f.UserID,
			TileX:     f.TileX,
			TileY:     f.TileY,
			PX:        f.PX,
			PY:        f.PY,
			Direction: f.Direction,
			Sitting:   f.Sitting,
		}); err == nil {
			r.broadcastExcept(msg, f.UserID)
		}
		node = f
	}
}

// runMoveTicker flushes buffered moves to peers every 20ms (was 50ms).
// Reducing the tick from 50→20ms cuts the server-side pipeline latency from
// up to 50ms to up to 20ms, giving clients fresher position data and making
// the 100ms interpolation buffer much less likely to run dry.
// Bandwidth is unchanged because pendingMoves uses latest-wins coalescing:
// each player contributes at most one binary frame per flush regardless of
// how many moves they sent since the last tick.
// It runs as a dedicated goroutine per Room and stops when stopTick is closed.
func (r *Room) runMoveTicker() {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.flushMoves()
		case <-r.stopTick:
			return
		}
	}
}

// flushMoves drains pendingMoves and broadcasts each player's latest position
// as a compact binary moved_bin frame (WebSocket BinaryMessage).
// Latest-wins coalescing: if a player sent N moves since the last tick, only
// the most recent position is sent — intermediate steps are intentionally dropped.
func (r *Room) flushMoves() {
	r.pendingMoves.Range(func(k, v any) bool {
		r.pendingMoves.Delete(k)
		entry := v.(pendingMoveEntry)
		msg := encodeBinMoved(entry.moved) // never fails — use directly
		if entry.roomID != "" {
			r.broadcastToRoomBin(msg, entry.moved.UserID, entry.roomID)
		} else {
			for _, peer := range r.aoi.Subscribers(entry.moved.TileX, entry.moved.TileY, entry.moved.UserID) {
				peer.SendBin(msg)
			}
		}
		return true
	})
}

// shutdown stops the tick goroutine. Safe to call multiple times.
func (r *Room) shutdown() {
	r.stopTickOnce.Do(func() {
		close(r.stopTick)
	})
}

func (r *Room) handleChat(c *Client, payload json.RawMessage) {
	var p ClientChatPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid chat payload")
		return
	}

	if p.Content == "" {
		return
	}

	// Truncate to prevent oversized payloads.
	if utf8.RuneCountInString(p.Content) > maxChatLength {
		runes := []rune(p.Content)
		p.Content = string(runes[:maxChatLength])
	}

	if msg, err := encode(MsgChat, ChatPayload{
		UserID:      c.UserID,
		DisplayName: c.EffectiveName(),
		Content:     p.Content,
	}); err == nil {
		r.broadcast(msg) // including sender so they see their own message
	}
}

func (r *Room) handleStatus(c *Client, payload json.RawMessage) {
	var p ClientStatusPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid status payload")
		return
	}
	validStatuses := map[string]bool{"available": true, "busy": true, "away": true, "dnd": true}
	if !validStatuses[p.Status] {
		r.sendError(c, "status must be available|busy|away|dnd")
		return
	}
	if len([]rune(p.CustomMsg)) > 30 {
		p.CustomMsg = string([]rune(p.CustomMsg)[:30])
	}
	c.Status = p.Status
	c.CustomMsg = p.CustomMsg

	// Update Redis presence with new status.
	if s := r.redisStore(); s != nil {
		ctx := context.Background()
		_ = s.SetPresence(ctx, r.workspaceID, c.UserID, c.DisplayName, c.AvatarURL, c.Status)
	}

	if msg, err := encode(MsgStatusChanged, StatusChangedPayload{
		UserID:    c.UserID,
		Status:    c.Status,
		CustomMsg: c.CustomMsg,
	}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

func (r *Room) handleRoomEnter(c *Client, payload json.RawMessage) {
	var p ClientRoomPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid room_enter payload")
		return
	}
	if p.RoomID == "" {
		r.sendError(c, "room_id required")
		return
	}
	c.RoomID = p.RoomID

	// Persist room assignment to Redis.
	if s := r.redisStore(); s != nil {
		_ = s.SetRoom(context.Background(), r.workspaceID, c.UserID, c.RoomID)
	}

	if msg, err := encode(MsgRoomEntered, RoomChangedPayload{UserID: c.UserID, RoomID: c.RoomID}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

func (r *Room) handleRoomExit(c *Client, _ json.RawMessage) {
	c.RoomID = ""

	if s := r.redisStore(); s != nil {
		_ = s.ClearRoom(context.Background(), r.workspaceID, c.UserID)
	}

	// Include current position so peers can place the player correctly when
	// their sprite reappears (prevents a brief snap to stale pre-room position).
	if msg, err := encode(MsgRoomExited, RoomChangedPayload{
		UserID: c.UserID,
		RoomID: "",
		TileX:  c.TileX,
		TileY:  c.TileY,
		PX:     c.PX,
		PY:     c.PY,
	}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

func (r *Room) handleWave(c *Client, payload json.RawMessage) {
	var p ClientWavePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid wave payload")
		return
	}
	if p.TargetUserID == "" || p.TargetUserID == c.UserID {
		r.sendError(c, "invalid target_user_id")
		return
	}

	// Check cooldown — prefer Redis, fall back to in-memory.
	ctx := context.Background()
	if s := r.redisStore(); s != nil {
		onCD, err := s.WaveOnCooldown(ctx, r.workspaceID, c.UserID, p.TargetUserID)
		if err == nil && onCD {
			r.sendError(c, "wave cooldown active")
			return
		}
	} else {
		cdKey := c.UserID + ":" + p.TargetUserID
		if v, ok := r.waveCooldowns.Load(cdKey); ok {
			if time.Now().Before(v.(time.Time)) {
				r.sendError(c, "wave cooldown active")
				return
			}
		}
	}

	target, ok := r.getClient(p.TargetUserID)
	if !ok {
		r.sendError(c, "target not in office")
		return
	}

	// Set cooldown.
	if s := r.redisStore(); s != nil {
		_ = s.SetWaveCooldown(ctx, r.workspaceID, c.UserID, p.TargetUserID)
	} else {
		cdKey := c.UserID + ":" + p.TargetUserID
		r.waveCooldowns.Store(cdKey, time.Now().Add(10*time.Second))
	}

	// Broadcast wave animation to everyone in the room so all players see the sender waving.
	if animMsg, err := encode(MsgWaveAnimation, WaveAnimationPayload{UserID: c.UserID}); err == nil {
		r.broadcast(animMsg)
	}

	// Deliver wave notification only if the target has not opted out of interruptions.
	// Both "dnd" (do not disturb) and "busy" suppress the popup — the animation
	// still broadcasts to everyone so the sender's gesture remains visible.
	if target.Status == "dnd" || target.Status == "busy" {
		return
	}

	if msg, err := encode(MsgWaveReceived, WaveReceivedPayload{
		SenderUserID:    c.UserID,
		SenderName:      c.EffectiveName(),
		SenderAvatarURL: c.AvatarURL,
	}); err == nil {
		target.Send(msg)
	}
}

// maxFollowChain bounds chain traversal so a corrupt cycle can never spin forever.
const maxFollowChain = 256

// sendMsg encodes and unicasts a message to one client (best-effort).
func (r *Room) sendMsg(c *Client, msgType string, payload any) {
	if msg, err := encode(msgType, payload); err == nil {
		c.Send(msg)
	}
}

// persistFollow mirrors a client's ahead-pointer to Redis (for reconnect resume).
func (r *Room) persistFollow(c *Client, targetID string) {
	if s := r.redisStore(); s != nil {
		_ = s.SetFollow(context.Background(), r.workspaceID, c.UserID, targetID)
	}
}

func (r *Room) broadcastFollowChanged(c *Client) {
	if msg, err := encode(MsgFollowChanged, FollowChangedPayload{
		UserID:         c.UserID,
		FollowTargetID: c.FollowTargetID,
	}); err == nil {
		r.broadcast(msg)
	}
}

func followPayload(c *Client, following bool) FollowPayload {
	return FollowPayload{
		FollowerUserID: c.UserID,
		FollowerName:   c.EffectiveName(),
		FollowerAvatar: c.AvatarURL,
		Following:      following,
	}
}

// resolveTail walks DOWN the followedBy links to the last node in the chain.
// Stale pointers (the next node already left) are repaired in place.
func (r *Room) resolveTail(start *Client) *Client {
	node := start
	for i := 0; node.FollowerID != "" && i < maxFollowChain; i++ {
		next, ok := r.getClient(node.FollowerID)
		if !ok {
			node.FollowerID = "" // repair dangling tail
			break
		}
		node = next
	}
	return node
}

// resolveLeader walks UP the following links to the head of the chain.
func (r *Room) resolveLeader(start *Client) *Client {
	node := start
	for i := 0; node.FollowTargetID != "" && i < maxFollowChain; i++ {
		prev, ok := r.getClient(node.FollowTargetID)
		if !ok {
			node.FollowTargetID = ""
			break
		}
		node = prev
	}
	return node
}

// detach removes c from its follow chain and heals the gap:
//
//	...ahead <- c <- behind...   becomes   ...ahead <- behind...
//
// The trailing node is re-pointed at the upstream neighbour (follow_assigned), or
// told to stop if the chain head is gone (follow_revoked). Safe to call when c has
// no links, and safe after c was removed from the clients map (disconnect path).
func (r *Room) detach(c *Client) {
	aheadID, behindID := c.FollowTargetID, c.FollowerID
	if aheadID == "" && behindID == "" {
		return
	}

	var ahead, behind *Client
	if aheadID != "" {
		ahead, _ = r.getClient(aheadID)
	}
	if behindID != "" {
		behind, _ = r.getClient(behindID)
	}

	// Re-link the two neighbours directly to each other.
	if ahead != nil {
		ahead.FollowerID = behindID
	}
	if behind != nil {
		behind.FollowTargetID = aheadID
		r.persistFollow(behind, aheadID)
	}

	// Clear c's own links.
	c.FollowTargetID = ""
	c.FollowerID = ""
	r.persistFollow(c, "")

	// Update the node ahead about its direct-follower change (for follow badges).
	if ahead != nil {
		r.sendMsg(ahead, MsgFollowEnded, followPayload(c, false))
		if behind != nil {
			r.sendMsg(ahead, MsgFollowStarted, followPayload(behind, true))
		}
	}

	// Heal the trailing node: follow the upstream neighbour, or stop if none.
	if behind != nil {
		if aheadID != "" {
			r.sendMsg(behind, MsgFollowAssigned, FollowAssignedPayload{
				TargetUserID: aheadID,
				ChainLeader:  r.resolveLeader(behind).UserID,
			})
			r.broadcastFollowChanged(behind)
		} else {
			r.sendMsg(behind, MsgFollowRevoked, followPayload(behind, false))
			r.broadcastFollowChanged(behind)
		}
	}

	// Broadcast c's follow_target cleared.
	r.broadcastFollowChanged(c)
}

// handleFollow binds c into a follow chain. A new follower is appended at the TAIL
// of the target's line instead of crowding the target directly.
func (r *Room) handleFollow(c *Client, payload json.RawMessage) {
	var p ClientFollowPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid follow payload")
		return
	}

	// Serialise with detach/cascade so chain links are never mutated concurrently.
	r.followMu.Lock()
	defer r.followMu.Unlock()

	// Empty target = unfollow: leave the chain and heal it.
	if p.TargetUserID == "" {
		r.detach(c)
		return
	}
	if p.TargetUserID == c.UserID {
		r.sendError(c, "cannot follow yourself")
		return
	}
	target, ok := r.getClient(p.TargetUserID)
	if !ok {
		r.sendError(c, "target not in office")
		return
	}
	// Respect the target's availability — busy and dnd users decline unsolicited follows.
	if target.Status == "busy" || target.Status == "dnd" {
		r.sendError(c, "user is not available for following")
		return
	}

	// Remember who was following c BEFORE detaching, so we can re-attach them
	// to c in the new chain (conga-line stays intact when a middle node re-targets).
	oldFollowerID := c.FollowerID

	// Leave any current chain first. This also guarantees c is absent from the
	// target's chain, so appending below can never create a cycle.
	r.detach(c)

	// Append to the TAIL of the target's follow-line (conga-line behaviour).
	tail := r.resolveTail(target)
	if tail.UserID == c.UserID {
		return // resolved back to ourselves — nothing to do
	}

	c.FollowTargetID = tail.UserID
	tail.FollowerID = c.UserID
	r.persistFollow(c, tail.UserID)

	// Tell c which node to actually walk behind (the tail), plus the chain leader.
	r.sendMsg(c, MsgFollowAssigned, FollowAssignedPayload{
		TargetUserID: tail.UserID,
		ChainLeader:  r.resolveLeader(c).UserID,
	})
	// Notify the tail that it gained a direct follower.
	r.sendMsg(tail, MsgFollowStarted, followPayload(c, true))

	// Broadcast to all: c's follow target changed (for context menu UI hints).
	r.broadcastFollowChanged(c)

	// Re-attach the old follower behind c so the chain stays connected.
	// Example: A→B, then B follows C → chain should become C←B←A, not C←B + A revoked.
	// Guard: skip if the old follower is already part of the new chain (prevent cycles).
	if oldFollowerID != "" && oldFollowerID != target.UserID {
		// Walk the new chain upward from c to check the old follower isn't already in it.
		inChain := false
		node := c
		for i := 0; node.FollowTargetID != "" && i < maxFollowChain; i++ {
			if node.FollowTargetID == oldFollowerID {
				inChain = true
				break
			}
			n, ok := r.getClient(node.FollowTargetID)
			if !ok {
				break
			}
			node = n
		}
		if !inChain {
			behind, ok := r.getClient(oldFollowerID)
			if ok && behind.FollowTargetID == "" {
				// behind was revoked by detach — re-link it behind c.
				behind.FollowTargetID = c.UserID
				c.FollowerID = behind.UserID
				r.persistFollow(behind, c.UserID)
				r.sendMsg(behind, MsgFollowAssigned, FollowAssignedPayload{
					TargetUserID: c.UserID,
					ChainLeader:  r.resolveLeader(behind).UserID,
				})
				r.sendMsg(c, MsgFollowStarted, followPayload(behind, true))
				r.broadcastFollowChanged(behind)
			}
		}
	}
}

// handleStopFollower lets the FOLLOWEE kick a specific direct follower.
// The follower leaves the chain (which heals around it) and receives follow_revoked.
func (r *Room) handleStopFollower(c *Client, payload json.RawMessage) {
	var p ClientStopFollowerPayload
	if err := json.Unmarshal(payload, &p); err != nil || p.FollowerUserID == "" {
		r.sendError(c, "invalid stop_follower payload")
		return
	}

	follower, ok := r.getClient(p.FollowerUserID)
	if !ok {
		return
	}

	r.followMu.Lock()
	defer r.followMu.Unlock()
	// Only the direct followee may kick this follower.
	if follower.FollowTargetID != c.UserID {
		return
	}

	// detach heals the chain (follower's trailing node reconnects to c) and notifies
	// c via follow_ended/started. Then tell the kicked follower itself to stop.
	r.detach(follower)
	r.sendMsg(follower, MsgFollowRevoked, followPayload(follower, false))
}

// handleVisibility records the client's tab visibility (Page Visibility API). A
// hidden follower is walked by the server (cascadeHiddenFollowers); on return it
// resumes driving itself, so we snap it to its authoritative position first.
func (r *Room) handleVisibility(c *Client, payload json.RawMessage) {
	var p ClientVisibilityPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return
	}
	r.followMu.Lock()
	changed := c.Hidden != p.Hidden
	c.Hidden = p.Hidden
	r.followMu.Unlock()
	slog.Info("[visibility]",
		"user", c.UserID,
		"hidden", p.Hidden,
		"changed", changed,
		"followTarget", c.FollowTargetID,
		"follower", c.FollowerID,
	)
	if changed && !p.Hidden && c.FollowTargetID != "" {
		r.forceSync(c, "visibility_resume")
	}
}

// cascadeHiddenFollowers walks the server-driven (hidden) followers behind `mover`
// one tile behind it, mirroring the path the mover just travelled. It stops at the
// first foreground follower, whose own client drives it reactively from the broadcast.
// `path` is the tile path the mover just travelled (leader→tail order, >= 2 tiles).
func (r *Room) cascadeHiddenFollowers(mover *Client, path []TilePoint) {
	if len(path) < 2 {
		return
	}
	r.followMu.Lock()
	defer r.followMu.Unlock()

	p := mover
	travelled := path
	for i := 0; i < maxFollowChain; i++ {
		if p.FollowerID == "" {
			slog.Info("[cascade] no follower", "node", p.UserID)
			return
		}
		f, ok := r.getClient(p.FollowerID)
		if !ok {
			slog.Info("[cascade] follower not found", "followerID", p.FollowerID)
			return
		}
		if !f.Hidden {
			slog.Info("[cascade] follower is foreground — skipping",
				"follower", f.UserID, "hidden", f.Hidden)
			return
		}
		slog.Info("[cascade] driving hidden follower",
			"follower", f.UserID, "predPath", len(travelled))
		next := r.driveServerFollow(f, travelled)
		if next == nil {
			return
		}
		p = f
		travelled = next
	}
}

// driveServerFollow walks a hidden follower one tile behind its predecessor by
// mirroring the predecessor's path minus its last tile. Broadcasts a "moving" for the
// follower and returns the follower's own travelled path so the cascade can drive the
// next hidden node. Returns nil if no movement was produced. Caller must hold followMu.
func (r *Room) driveServerFollow(f *Client, predPath []TilePoint) []TilePoint {
	if len(predPath) < 2 {
		return nil
	}
	// The follower occupies the tiles the predecessor vacated (its path minus the
	// final tile), ending one tile behind the predecessor.
	vacated := predPath[:len(predPath)-1]
	dest := vacated[len(vacated)-1]

	// If the follower is still mid-walk from a previous server-driven movement,
	// reconcile its authoritative position to where it actually is RIGHT NOW
	// along that path. Without this, `start` would be the previous destination
	// (set eagerly below) which peers haven't reached yet — the new movement
	// would begin from a phantom tile and peers see a positional jump.
	if f.IsMoving && len(f.MovePath) >= 2 {
		cur := currentPathTile(f.MovePath, f.MoveDurationMs, f.MoveStartedAt)
		f.TileX = cur.TileX
		f.TileY = cur.TileY
		f.PX, f.PY = tileCenterPx(cur.TileX, cur.TileY)
	}

	if f.TileX == dest.TileX && f.TileY == dest.TileY {
		return nil // already one tile behind
	}

	start := TilePoint{TileX: f.TileX, TileY: f.TileY}
	var fPath []TilePoint
	switch {
	case start.TileX == vacated[0].TileX && start.TileY == vacated[0].TileY:
		fPath = vacated // already on the first vacated tile
	case adjacentTiles(start, vacated[0]):
		fPath = append([]TilePoint{start}, vacated...) // tight line — walk the trail
	default:
		// Fell behind / just backgrounded far. Without server-side A* we can't smoothly
		// path around obstacles, so snap straight to one-behind. The follower's own
		// screen is hidden; downstream peers just see a quick catch-up.
		fPath = []TilePoint{start, dest}
	}

	prevTX, prevTY := f.TileX, f.TileY
	if !f.IsMoving {
		f.MoveStartTX, f.MoveStartTY = prevTX, prevTY
	}
	f.IsMoving = true
	f.MovePath = fPath
	f.MoveSpeed = playerSpeed
	f.MoveDurationMs = pathDurationMs(fPath, playerSpeed)
	f.MoveStartedAt = time.Now()
	f.TileX, f.TileY = dest.TileX, dest.TileY
	f.PX, f.PY = tileCenterPx(dest.TileX, dest.TileY)
	f.Sitting = false
	f.Direction = directionFromPath(fPath)

	if f.MoveDurationMs > 0 {
		if msg, err := encode(MsgMoving, MovingPayload{
			UserID:       f.UserID,
			Path:         fPath,
			DurationMs:   f.MoveDurationMs,
			Speed:        playerSpeed,
			AvatarURL:    f.AvatarURL,
			ServerTimeMs: f.MoveStartedAt.UnixMilli(),
		}); err == nil {
			r.broadcastExcept(msg, f.UserID)
		}
	}
	r.aoi.Move(f, f.TileX, f.TileY)
	return fPath
}

// directionFromPath derives a facing direction from the last two tiles of a path.
func directionFromPath(path []TilePoint) string {
	if len(path) < 2 {
		return ""
	}
	prev := path[len(path)-2]
	last := path[len(path)-1]
	dx := last.TileX - prev.TileX
	dy := last.TileY - prev.TileY
	switch {
	case dy < 0:
		return "up"
	case dy > 0:
		return "down"
	case dx < 0:
		return "left"
	case dx > 0:
		return "right"
	default:
		return ""
	}
}

// adjacentTiles reports whether a and b are within one tile (8-neighbourhood) and not equal.
func adjacentTiles(a, b TilePoint) bool {
	dx := absInt(a.TileX - b.TileX)
	dy := absInt(a.TileY - b.TileY)
	return dx <= 1 && dy <= 1 && dx+dy > 0
}

func (r *Room) handleKnock(c *Client, payload json.RawMessage) {
	var p ClientKnockPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid knock payload")
		return
	}
	if p.ZoneID == "" {
		r.sendError(c, "zone_id required")
		return
	}

	ctx := context.Background()

	// Check knock cooldown via Redis (if available).
	if s := r.redisStore(); s != nil {
		onCD, err := s.KnockOnCooldown(ctx, r.workspaceID, p.ZoneID, c.UserID)
		if err == nil && onCD {
			remaining, _ := s.KnockCooldownRemaining(ctx, r.workspaceID, p.ZoneID, c.UserID)
			r.sendError(c, fmt.Sprintf("knock cooldown: %.0fs remaining", remaining.Seconds()))
			return
		}
	}

	// Cancel any existing pending knock from this user+zone to prevent duplicate
	// notifications on the occupants' side (e.g. requester reloaded and knocked again).
	var oldRequestID string
	r.knockPending.Range(func(key, val any) bool {
		entry := val.(knockEntry)
		if entry.requesterID == c.UserID && entry.zoneID == p.ZoneID {
			oldRequestID = key.(string)
			return false
		}
		return true
	})
	if oldRequestID != "" {
		r.knockPending.Delete(oldRequestID)
		if msg, err := encode(MsgKnockCancelled, KnockCancelledPayload{RequestID: oldRequestID, ZoneID: p.ZoneID}); err == nil {
			r.broadcast(msg)
		}
	}

	requestID := generateRequestID()
	r.knockPending.Store(requestID, knockEntry{requesterID: c.UserID, zoneID: p.ZoneID})

	if s := r.redisStore(); s != nil {
		// Persist requester's "waiting" state for reload restoration.
		_ = s.SetKnockPending(ctx, r.workspaceID, p.ZoneID, c.UserID, requestID)
		// Persist full knock payload so occupants can restore notifications on reload.
		_ = s.SetKnockRequestData(ctx, r.workspaceID, requestID, store.KnockRequestData{
			ZoneID:          p.ZoneID,
			RequesterUserID: c.UserID,
			RequesterName:   c.EffectiveName(),
			RequesterAvatar: c.AvatarURL,
		})
	}

	// Broadcast knock_request to everyone in the room (occupants filter by zone client-side).
	if msg, err := encode(MsgKnockRequest, KnockRequestPayload{
		RequestID:       requestID,
		ZoneID:          p.ZoneID,
		RequesterUserID: c.UserID,
		RequesterName:   c.EffectiveName(),
		RequesterAvatar: c.AvatarURL,
	}); err == nil {
		r.broadcastExcept(msg, c.UserID)
	}
}

func (r *Room) handleKnockDecide(c *Client, payload json.RawMessage) {
	var p ClientKnockDecidePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid knock_decision payload")
		return
	}
	v, ok := r.knockPending.LoadAndDelete(p.RequestID)
	if !ok {
		r.sendError(c, "knock request not found or expired")
		return
	}
	entry := v.(knockEntry)
	requester, ok := r.getClient(entry.requesterID)
	if !ok {
		return // requester already left
	}

	ctx := context.Background()

	// Clear all Redis state for this knock now that a decision has been made.
	if s := r.redisStore(); s != nil {
		_ = s.DeleteKnockPending(ctx, r.workspaceID, entry.zoneID, entry.requesterID)
		_ = s.DeleteKnockRequestData(ctx, r.workspaceID, p.RequestID)
	}

	if p.Allow {
		// Open the barrier — set zone_granted key in Redis.
		if s := r.redisStore(); s != nil {
			_ = s.SetZoneGranted(ctx, r.workspaceID, entry.zoneID, entry.requesterID)
		}
		if msg, err := encode(MsgKnockGranted, KnockResultPayload{
			RequestID: p.RequestID,
			ZoneID:    entry.zoneID,
			Granted:   true,
		}); err == nil {
			requester.Send(msg)
		}
	} else {
		// Deny — increment counter and set cooldown.
		cooldownSec := 30
		var denyCount int64
		if s := r.redisStore(); s != nil {
			denyCount, _ = s.IncrementDenyCount(ctx, r.workspaceID, entry.zoneID, entry.requesterID)
			_ = s.SetKnockCooldown(ctx, r.workspaceID, entry.zoneID, entry.requesterID, denyCount)
		} else {
			denyCount = r.incrementDenyCount(entry.zoneID, entry.requesterID)
		}
		if denyCount >= 3 {
			cooldownSec = 300 // 5 minutes
		}
		if msg, err := encode(MsgKnockDenied, KnockDeniedPayload{
			RequestID:    p.RequestID,
			ZoneID:       entry.zoneID,
			CooldownSec:  cooldownSec,
			DenierUserID: c.UserID,
			DenierName:   c.EffectiveName(),
			DenierAvatar: c.AvatarURL,
		}); err == nil {
			requester.Send(msg)
		}
	}

	// Broadcast to ALL room occupants so they can dismiss the pending knock notification.
	// This ensures other people inside the zone don't see a stale "Accept/Deny" card
	// after someone has already made a decision.
	if msg, err := encode(MsgKnockDecided, KnockDecidedPayload{
		RequestID: p.RequestID,
		ZoneID:    entry.zoneID,
	}); err == nil {
		r.broadcast(msg)
	}
}

// handleKnockCancel is called when the requester cancels their pending knock.
// It finds the pending knock by user ID + zone ID, removes it, and broadcasts
// knock_cancelled to all room occupants so they can dismiss the notification.
func (r *Room) handleKnockCancel(c *Client, payload json.RawMessage) {
	var p ClientKnockCancelPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid knock_cancel payload")
		return
	}
	if p.ZoneID == "" {
		r.sendError(c, "zone_id required")
		return
	}

	// Find the pending knock for this requester + zone.
	var foundRequestID string
	r.knockPending.Range(func(key, val any) bool {
		entry := val.(knockEntry)
		if entry.requesterID == c.UserID && entry.zoneID == p.ZoneID {
			foundRequestID = key.(string)
			return false // stop iteration
		}
		return true
	})

	if foundRequestID == "" {
		// Knock already decided or never existed — nothing to do.
		return
	}

	r.knockPending.Delete(foundRequestID)

	// Clear all Redis state for this knock.
	if s := r.redisStore(); s != nil {
		ctx := context.Background()
		_ = s.DeleteKnockPending(ctx, r.workspaceID, p.ZoneID, c.UserID)
		_ = s.DeleteKnockRequestData(ctx, r.workspaceID, foundRequestID)
	}

	// Broadcast knock_cancelled to all occupants so they can dismiss the notification.
	if msg, err := encode(MsgKnockCancelled, KnockCancelledPayload{
		RequestID: foundRequestID,
		ZoneID:    p.ZoneID,
	}); err == nil {
		r.broadcast(msg)
	}

	slog.Info("knock cancelled", "workspace", r.workspaceID, "zone", p.ZoneID, "requester", c.UserID)
}

// handlePing replies with a pong carrying the server's wall-clock time and the
// echoed client time. The client uses this (with the round-trip time) to compute
// the server↔client clock offset for server-anchored movement interpolation.
func (r *Room) handlePing(c *Client, payload json.RawMessage) {
	var p ClientPingPayload
	// Tolerate empty/legacy ping payloads ("{}" or absent) — zero value is fine.
	_ = json.Unmarshal(payload, &p)
	if msg, err := encode(MsgPong, PongPayload{
		ServerTimeMs: time.Now().UnixMilli(),
		ClientTimeMs: p.ClientTimeMs,
	}); err == nil {
		c.Send(msg)
	}
}

func (r *Room) handleHeartbeat(c *Client) {
	if s := r.redisStore(); s != nil {
		ctx := context.Background()
		_ = s.RefreshPresence(ctx, r.workspaceID, c.UserID)
	}
}

// broadcastToRoom sends msg to all clients currently inside roomID, except excludeUserID.
// Used by handleMove tier-1 AOI: only roommates receive movement updates.
func (r *Room) broadcastToRoom(msg []byte, excludeUserID, roomID string) {
	r.clients.Range(func(k, v any) bool {
		if k.(string) != excludeUserID {
			if c := v.(*Client); c.RoomID == roomID {
				c.Send(msg)
			}
		}
		return true
	})
}

// broadcastToRoomBin sends a binary frame to all clients in a named room except excludeUserID.
func (r *Room) broadcastToRoomBin(msg []byte, excludeUserID, roomID string) {
	r.clients.Range(func(k, v any) bool {
		if k.(string) != excludeUserID {
			if c := v.(*Client); c.RoomID == roomID {
				c.SendBin(msg)
			}
		}
		return true
	})
}

// broadcast sends a message to every client in the room.
func (r *Room) broadcast(msg []byte) {
	r.clients.Range(func(_, v any) bool {
		v.(*Client).Send(msg)
		return true
	})
}

// broadcastExcept sends a message to all clients except excludeUserID.
func (r *Room) broadcastExcept(msg []byte, excludeUserID string) {
	r.clients.Range(func(k, v any) bool {
		if k.(string) != excludeUserID {
			v.(*Client).Send(msg)
		}
		return true
	})
}

// players returns a snapshot of all players in the room, excluding excludeUserID.
func (r *Room) players(excludeUserID string) []Player {
	list := []Player{}
	r.clients.Range(func(_, v any) bool {
		c := v.(*Client)
		if c.UserID != excludeUserID {
			list = append(list, c.Player())
		}
		return true
	})
	return list
}

// count returns the number of connected clients.
func (r *Room) count() int {
	n := 0
	r.clients.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (r *Room) sendError(c *Client, message string) {
	if msg, err := encode(MsgError, ErrorPayload{Message: message}); err == nil {
		c.Send(msg)
	}
}

// getObstacleGrid lazily loads (and caches for the room's lifetime) the obstacle
// grid published by zyra-api. Cached once: a re-publish only affects new rooms,
// which is acceptable since rooms are torn down when empty.
func (r *Room) getObstacleGrid() *store.ObstacleGrid {
	r.obstacleMu.Lock()
	defer r.obstacleMu.Unlock()
	if !r.obstacleLoadedAt.IsZero() && time.Since(r.obstacleLoadedAt) < obstacleTTL {
		return r.obstacleGrid
	}
	r.obstacleLoadedAt = time.Now()
	if s := r.redisStore(); s != nil {
		if g, err := s.GetWorkspaceObstacles(context.Background(), r.workspaceID); err == nil {
			r.obstacleGrid = g
		}
	}
	return r.obstacleGrid
}

// tileBlocked reports whether a destination tile is out of bounds or sits on a
// blocked tile per the published grid. A nil grid never blocks (fail-open).
func (r *Room) tileBlocked(g *store.ObstacleGrid, tx, ty int) bool {
	if g == nil {
		return false
	}
	if g.W > 0 && g.H > 0 && (tx < 0 || ty < 0 || tx >= g.W || ty >= g.H) {
		return true
	}
	if len(g.Blocked) > 0 {
		if _, ok := g.Blocked[fmt.Sprintf("%d,%d", tx, ty)]; ok {
			return true
		}
	}
	return false
}

// pathCrossesWall checks whether any step in the tile path crosses a wall edge.
// Validates both: exiting a wall tile through its blocked side, and entering a
// wall tile from its blocked (hard-approach) side.
func (r *Room) pathCrossesWall(g *store.ObstacleGrid, path []TilePoint) bool {
	if g == nil || len(g.Walls) == 0 || len(path) < 2 {
		return false
	}
	for i := 0; i < len(path)-1; i++ {
		dx := path[i+1].TileX - path[i].TileX
		dy := path[i+1].TileY - path[i].TileY

		// Check exit: wall on the source tile blocks leaving in that direction
		fromKey := fmt.Sprintf("%d,%d", path[i].TileX, path[i].TileY)
		if wallDir, ok := g.Walls[fromKey]; ok {
			if isWallBlockedExitGo(wallDir, dx, dy) {
				return true
			}
		}

		// Check entry: wall on the destination tile blocks approach from this direction
		toKey := fmt.Sprintf("%d,%d", path[i+1].TileX, path[i+1].TileY)
		if wallDir, ok := g.Walls[toKey]; ok {
			if isWallHardApproachGo(wallDir, dx, dy) {
				return true
			}
		}
	}
	return false
}

// isWallHardApproachGo checks if entering a wall tile from direction (dx,dy) is blocked.
// Mirrors client-side isWallHardApproach: wall blocks approach FROM its own side.
func isWallHardApproachGo(wallDir string, dx, dy int) bool {
	hasTop := wallDir == "top" || wallDir == "top_left" || wallDir == "top_right"
	hasBottom := wallDir == "bottom" || wallDir == "bottom_left" || wallDir == "bottom_right"
	hasLeft := wallDir == "left" || wallDir == "top_left" || wallDir == "bottom_left"
	hasRight := wallDir == "right" || wallDir == "top_right" || wallDir == "bottom_right"
	if dy > 0 && hasTop {
		return true
	}
	if dy < 0 && hasBottom {
		return true
	}
	if dx > 0 && hasLeft {
		return true
	}
	if dx < 0 && hasRight {
		return true
	}
	return false
}

// isWallBlockedExitGo checks if exiting in direction (dx,dy) is blocked by wallDir.
func isWallBlockedExitGo(wallDir string, dx, dy int) bool {
	hasTop := wallDir == "top" || wallDir == "top_left" || wallDir == "top_right"
	hasBottom := wallDir == "bottom" || wallDir == "bottom_left" || wallDir == "bottom_right"
	hasLeft := wallDir == "left" || wallDir == "top_left" || wallDir == "bottom_left"
	hasRight := wallDir == "right" || wallDir == "top_right" || wallDir == "bottom_right"
	if dx > 0 && hasRight {
		return true
	}
	if dx < 0 && hasLeft {
		return true
	}
	if dy < 0 && hasTop {
		return true
	}
	if dy > 0 && hasBottom {
		return true
	}
	return false
}

// forceSync tells the client to snap its local player back to the server's
// authoritative position. Called after rejecting an invalid move so the client
// cannot retain a position the server never accepted (reconciliation).
func (r *Room) forceSync(c *Client, reason string) {
	px, py := c.PX, c.PY
	if px == 0 && py == 0 {
		px, py = tileCenterPx(c.TileX, c.TileY)
	}
	if msg, err := encode(MsgForceSync, ForceSyncPayload{
		TileX:     c.TileX,
		TileY:     c.TileY,
		PX:        px,
		PY:        py,
		Direction: c.Direction,
		Reason:    reason,
	}); err == nil {
		c.Send(msg)
	}
}

// getClient retrieves a connected client by userID.
func (r *Room) getClient(userID string) (*Client, bool) {
	v, ok := r.clients.Load(userID)
	if !ok {
		return nil, false
	}
	return v.(*Client), true
}

// generateRequestID returns a short unique ID for knock requests based on current time.
func generateRequestID() string {
	t := time.Now().UnixNano()
	return fmt.Sprintf("%x", t)
}

// handleSectionSync relays a section state update to every client in the room.
// The WS server acts as a pure relay here — business logic lives in zyra-api.
func (r *Room) handleSectionSync(c *Client, payload json.RawMessage) {
	var p SectionSyncPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		r.sendError(c, "invalid section_sync payload")
		return
	}
	msg, err := encode(MsgSectionSync, p)
	if err != nil {
		return
	}
	r.broadcast(msg)
}
