// Package store wraps Redis operations for the virtual-office service.
// All keys follow the vo: namespace defined in the technical design.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	presenceTTL    = 35 * time.Second // heartbeat every 20s → 35s gives safe margin
	knockCDBase    = 30 * time.Second // 1st/2nd knock cooldown
	knockCDLong    = 5 * time.Minute  // 3rd+ deny cooldown
	knockDenyTTL   = 10 * time.Minute // deny counter window
	waveCDTTL      = 10 * time.Second // wave cooldown per sender-target
	zoneGrantedTTL = 30 * time.Second // barrier open window after allow
	knockPendingTTL = 10 * time.Minute // pending knock survives page reload

	maxDeniesBeforeLong = 3 // deny count threshold before 5-min cooldown
)

// RedisStore provides key-based operations for virtual-office state.
type RedisStore struct {
	rdb *redis.Client
}

// New creates a RedisStore. Returns nil (no-op mode) if rdb is nil.
func New(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

// available reports whether Redis is configured.
func (s *RedisStore) available() bool {
	return s != nil && s.rdb != nil
}

// ── Presence ─────────────────────────────────────────────────────────────────

func presenceKey(wsID, userID string) string {
	return fmt.Sprintf("vo:presence:%s:%s", wsID, userID)
}

// SetPresence writes/refreshes the presence hash for a user.
func (s *RedisStore) SetPresence(ctx context.Context, wsID, userID, displayName, avatarURL, status string, tileX, tileY int) error {
	if !s.available() {
		return nil
	}
	key := presenceKey(wsID, userID)
	fields := map[string]any{
		"display_name": displayName,
		"avatar_url":   avatarURL,
		"status":       status,
		"tile_x":       tileX,
		"tile_y":       tileY,
	}
	pipe := s.rdb.Pipeline()
	pipe.HMSet(ctx, key, fields)
	pipe.Expire(ctx, key, presenceTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// RefreshPresence renews the TTL without overwriting other fields.
func (s *RedisStore) RefreshPresence(ctx context.Context, wsID, userID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Expire(ctx, presenceKey(wsID, userID), presenceTTL).Err()
}

// DeletePresence removes the presence key when a user leaves.
func (s *RedisStore) DeletePresence(ctx context.Context, wsID, userID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Del(ctx, presenceKey(wsID, userID)).Err()
}

// OnlineCount returns the number of active presence keys for a workspace.
func (s *RedisStore) OnlineCount(ctx context.Context, wsID string) (int, error) {
	if !s.available() {
		return 0, nil
	}
	pattern := fmt.Sprintf("vo:presence:%s:*", wsID)
	keys, err := s.rdb.Keys(ctx, pattern).Result()
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

// ── Room tracking ─────────────────────────────────────────────────────────────

func roomKey(wsID, userID string) string {
	return fmt.Sprintf("vo:room:%s:%s", wsID, userID)
}

// SetRoom writes the current room ID for a user.
func (s *RedisStore) SetRoom(ctx context.Context, wsID, userID, roomID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Set(ctx, roomKey(wsID, userID), roomID, presenceTTL).Err()
}

// ClearRoom removes the room key when a user leaves a room.
func (s *RedisStore) ClearRoom(ctx context.Context, wsID, userID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Del(ctx, roomKey(wsID, userID)).Err()
}

// ── Wave cooldown ─────────────────────────────────────────────────────────────

func waveKey(wsID, senderID, targetID string) string {
	return fmt.Sprintf("vo:wave_cd:%s:%s:%s", wsID, senderID, targetID)
}

// WaveOnCooldown returns true if the sender is on wave cooldown toward the target.
func (s *RedisStore) WaveOnCooldown(ctx context.Context, wsID, senderID, targetID string) (bool, error) {
	if !s.available() {
		return false, nil
	}
	exists, err := s.rdb.Exists(ctx, waveKey(wsID, senderID, targetID)).Result()
	return exists > 0, err
}

// SetWaveCooldown activates a wave cooldown.
func (s *RedisStore) SetWaveCooldown(ctx context.Context, wsID, senderID, targetID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Set(ctx, waveKey(wsID, senderID, targetID), "1", waveCDTTL).Err()
}

// ── Knock cooldown ────────────────────────────────────────────────────────────

func knockCDKey(wsID, zoneID, userID string) string {
	return fmt.Sprintf("vo:knock_cd:%s:%s:%s", wsID, zoneID, userID)
}

func knockDenyKey(wsID, zoneID, userID string) string {
	return fmt.Sprintf("vo:knock_deny_count:%s:%s:%s", wsID, zoneID, userID)
}

// KnockOnCooldown returns true if the user is currently on cooldown for this zone.
func (s *RedisStore) KnockOnCooldown(ctx context.Context, wsID, zoneID, userID string) (bool, error) {
	if !s.available() {
		return false, nil
	}
	exists, err := s.rdb.Exists(ctx, knockCDKey(wsID, zoneID, userID)).Result()
	return exists > 0, err
}

// SetKnockCooldown sets the cooldown after a deny, applying progressive logic.
// denyCount is the total denies AFTER the current one.
func (s *RedisStore) SetKnockCooldown(ctx context.Context, wsID, zoneID, userID string, denyCount int64) error {
	if !s.available() {
		return nil
	}
	ttl := knockCDBase
	if denyCount >= int64(maxDeniesBeforeLong) {
		ttl = knockCDLong
	}
	return s.rdb.Set(ctx, knockCDKey(wsID, zoneID, userID), "1", ttl).Err()
}

// IncrementDenyCount increments the deny counter and returns the new total.
func (s *RedisStore) IncrementDenyCount(ctx context.Context, wsID, zoneID, userID string) (int64, error) {
	if !s.available() {
		return 1, nil
	}
	pipe := s.rdb.Pipeline()
	incrCmd := pipe.Incr(ctx, knockDenyKey(wsID, zoneID, userID))
	pipe.Expire(ctx, knockDenyKey(wsID, zoneID, userID), knockDenyTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 1, err
	}
	return incrCmd.Val(), nil
}

// KnockCooldownRemaining returns the TTL of the knock cooldown key.
func (s *RedisStore) KnockCooldownRemaining(ctx context.Context, wsID, zoneID, userID string) (time.Duration, error) {
	if !s.available() {
		return 0, nil
	}
	return s.rdb.TTL(ctx, knockCDKey(wsID, zoneID, userID)).Result()
}

// ── Zone granted (barrier open) ───────────────────────────────────────────────

func zoneGrantedKey(wsID, zoneID, userID string) string {
	return fmt.Sprintf("vo:zone_granted:%s:%s:%s", wsID, zoneID, userID)
}

// SetZoneGranted records that a user was allowed into a private zone.
func (s *RedisStore) SetZoneGranted(ctx context.Context, wsID, zoneID, userID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Set(ctx, zoneGrantedKey(wsID, zoneID, userID), "1", zoneGrantedTTL).Err()
}

// ZoneGranted returns true if the user currently holds an open-barrier token.
func (s *RedisStore) ZoneGranted(ctx context.Context, wsID, zoneID, userID string) (bool, error) {
	if !s.available() {
		return false, nil
	}
	exists, err := s.rdb.Exists(ctx, zoneGrantedKey(wsID, zoneID, userID)).Result()
	return exists > 0, err
}

// ── Follow state ──────────────────────────────────────────────────────────────

func followKey(wsID, followerID string) string {
	return fmt.Sprintf("vo:follow:%s:%s", wsID, followerID)
}

// SetFollow stores the follow target for a user (empty = unfollow).
func (s *RedisStore) SetFollow(ctx context.Context, wsID, followerID, targetID string) error {
	if !s.available() {
		return nil
	}
	if targetID == "" {
		return s.rdb.Del(ctx, followKey(wsID, followerID)).Err()
	}
	// No TTL — session-lived; cleared on disconnect.
	return s.rdb.Set(ctx, followKey(wsID, followerID), targetID, 0).Err()
}

// DeleteFollow clears the follow state on disconnect.
func (s *RedisStore) DeleteFollow(ctx context.Context, wsID, followerID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.Del(ctx, followKey(wsID, followerID)).Err()
}

// ── Last position (LP-3) ─────────────────────────────────────────────────────

const lastPosTTL = 7 * 24 * time.Hour // 7 days — survives across sessions

func lastPosKey(wsID, userID string) string {
	return fmt.Sprintf("vo:last_pos:%s:%s", wsID, userID)
}

// SetLastPosition persists the user's last tile when they disconnect.
func (s *RedisStore) SetLastPosition(ctx context.Context, wsID, userID string, tileX, tileY int) error {
	if !s.available() {
		return nil
	}
	return s.rdb.HSet(ctx, lastPosKey(wsID, userID),
		"tile_x", tileX,
		"tile_y", tileY,
	).Err()
}

// GetLastPosition retrieves the user's last recorded tile position (0,0 if not set).
func (s *RedisStore) GetLastPosition(ctx context.Context, wsID, userID string) (tileX, tileY int, err error) {
	if !s.available() {
		return 0, 0, nil
	}
	vals, e := s.rdb.HMGet(ctx, lastPosKey(wsID, userID), "tile_x", "tile_y").Result()
	if e != nil {
		return 0, 0, e
	}
	if vals[0] != nil {
		tileX = ParseInt(fmt.Sprintf("%v", vals[0]))
	}
	if vals[1] != nil {
		tileY = ParseInt(fmt.Sprintf("%v", vals[1]))
	}
	// Reset TTL on access — extends the window
	_ = s.rdb.Expire(ctx, lastPosKey(wsID, userID), lastPosTTL)
	return tileX, tileY, nil
}

// ── Knock request data (full payload for reconnect restoration) ───────────────

// KnockRequestData holds all knock request fields needed to restore notifications.
type KnockRequestData struct {
	ZoneID          string `json:"zone_id"`
	RequesterUserID string `json:"requester_user_id"`
	RequesterName   string `json:"requester_name"`
	RequesterAvatar string `json:"requester_avatar"`
}

// KnockRequestEntry combines a requestID with its data for bulk retrieval.
type KnockRequestEntry struct {
	RequestID string
	KnockRequestData
}

func knockRequestKey(wsID, requestID string) string {
	return fmt.Sprintf("vo:knock_req:%s:%s", wsID, requestID)
}

func wsKnockIndexKey(wsID string) string {
	return fmt.Sprintf("vo:ws_knock_idx:%s", wsID)
}

// SetKnockRequestData persists the full knock request payload in Redis.
// The workspace-level set index is also updated for bulk lookup on welcome.
func (s *RedisStore) SetKnockRequestData(ctx context.Context, wsID, requestID string, data KnockRequestData) error {
	if !s.available() {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, knockRequestKey(wsID, requestID), raw, knockPendingTTL)
	pipe.SAdd(ctx, wsKnockIndexKey(wsID), requestID)
	pipe.Expire(ctx, wsKnockIndexKey(wsID), knockPendingTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// DeleteKnockRequestData removes a knock request and its index entry from Redis.
func (s *RedisStore) DeleteKnockRequestData(ctx context.Context, wsID, requestID string) error {
	if !s.available() {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, knockRequestKey(wsID, requestID))
	pipe.SRem(ctx, wsKnockIndexKey(wsID), requestID)
	_, err := pipe.Exec(ctx)
	return err
}

// GetWorkspaceKnockRequests returns all active knock requests for a workspace.
// Stale index entries (expired keys) are cleaned up automatically.
func (s *RedisStore) GetWorkspaceKnockRequests(ctx context.Context, wsID string) ([]KnockRequestEntry, error) {
	if !s.available() {
		return nil, nil
	}
	requestIDs, err := s.rdb.SMembers(ctx, wsKnockIndexKey(wsID)).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]KnockRequestEntry, 0, len(requestIDs))
	for _, requestID := range requestIDs {
		raw, err := s.rdb.Get(ctx, knockRequestKey(wsID, requestID)).Result()
		if errors.Is(err, redis.Nil) {
			// TTL expired — remove stale index entry silently.
			_ = s.rdb.SRem(ctx, wsKnockIndexKey(wsID), requestID)
			continue
		}
		if err != nil {
			continue
		}
		var data KnockRequestData
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			continue
		}
		entries = append(entries, KnockRequestEntry{RequestID: requestID, KnockRequestData: data})
	}
	return entries, nil
}

// ── Knock pending (persists across page reloads) ──────────────────────────────

func knockPendingKey(wsID, zoneID, userID string) string {
	return fmt.Sprintf("vo:knock_pending:%s:%s:%s", wsID, zoneID, userID)
}

func knockPendingUserSetKey(wsID, userID string) string {
	return fmt.Sprintf("vo:knock_pending_idx:%s:%s", wsID, userID)
}

// SetKnockPending stores the requestID for a pending knock with a TTL.
// A user-level set index is maintained for efficient per-user lookups.
func (s *RedisStore) SetKnockPending(ctx context.Context, wsID, zoneID, userID, requestID string) error {
	if !s.available() {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, knockPendingKey(wsID, zoneID, userID), requestID, knockPendingTTL)
	pipe.SAdd(ctx, knockPendingUserSetKey(wsID, userID), zoneID)
	pipe.Expire(ctx, knockPendingUserSetKey(wsID, userID), knockPendingTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// GetKnockPending returns the requestID for a pending knock, or "" if none.
func (s *RedisStore) GetKnockPending(ctx context.Context, wsID, zoneID, userID string) (string, error) {
	if !s.available() {
		return "", nil
	}
	val, err := s.rdb.Get(ctx, knockPendingKey(wsID, zoneID, userID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return val, err
}

// DeleteKnockPending removes the pending knock record (called on decide or cancel).
func (s *RedisStore) DeleteKnockPending(ctx context.Context, wsID, zoneID, userID string) error {
	if !s.available() {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, knockPendingKey(wsID, zoneID, userID))
	pipe.SRem(ctx, knockPendingUserSetKey(wsID, userID), zoneID)
	_, err := pipe.Exec(ctx)
	return err
}

// KnockPendingEntry holds the zone + requestID pair for a pending knock.
type KnockPendingEntry struct {
	ZoneID    string
	RequestID string
}

// GetPendingKnocks returns all active pending knocks for a user.
func (s *RedisStore) GetPendingKnocks(ctx context.Context, wsID, userID string) ([]KnockPendingEntry, error) {
	if !s.available() {
		return nil, nil
	}
	zoneIDs, err := s.rdb.SMembers(ctx, knockPendingUserSetKey(wsID, userID)).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]KnockPendingEntry, 0, len(zoneIDs))
	for _, zoneID := range zoneIDs {
		requestID, err := s.GetKnockPending(ctx, wsID, zoneID, userID)
		if err != nil || requestID == "" {
			// Stale index entry — remove it silently.
			_ = s.rdb.SRem(ctx, knockPendingUserSetKey(wsID, userID), zoneID)
			continue
		}
		entries = append(entries, KnockPendingEntry{ZoneID: zoneID, RequestID: requestID})
	}
	return entries, nil
}

// ── Position snapshot (pixel-accurate, for new joiners & multi-instance) ─────

func posSnapKey(wsID string) string {
	return fmt.Sprintf("vo:pos:%s", wsID)
}

// SavePosSnapshot stores a full position snapshot (JSON-encoded MovedPayload) for a player.
// Called on every move so new joiners receive pixel-accurate positions via the welcome message.
func (s *RedisStore) SavePosSnapshot(ctx context.Context, wsID, userID string, data []byte) error {
	if !s.available() {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, posSnapKey(wsID), userID, data)
	pipe.Expire(ctx, posSnapKey(wsID), presenceTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// GetAllPosSnapshots returns all stored position snapshots for a workspace.
func (s *RedisStore) GetAllPosSnapshots(ctx context.Context, wsID string) (map[string]string, error) {
	if !s.available() {
		return nil, nil
	}
	return s.rdb.HGetAll(ctx, posSnapKey(wsID)).Result()
}

// DeletePosSnapshot removes a player's position snapshot on disconnect.
func (s *RedisStore) DeletePosSnapshot(ctx context.Context, wsID, userID string) error {
	if !s.available() {
		return nil
	}
	return s.rdb.HDel(ctx, posSnapKey(wsID), userID).Err()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// ParseInt is a small utility to parse string → int.
func ParseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// ErrNoRedis is returned by operations that require Redis when it is unavailable.
var ErrNoRedis = errors.New("redis not configured")
