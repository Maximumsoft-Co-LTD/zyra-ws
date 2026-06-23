package hub

import "sync"

// AOICellSize is the tile width/height of each spatial grid cell.
// 16×16 tiles per cell means a 3×3 neighbourhood covers a 48×48 tile area
// (≈ 24-tile radius), matching the ~15-tile visibility target with a safety margin.
const AOICellSize = 16

type cellKey struct{ cx, cy int }

// AOIGrid is a coarse spatial index for the open floor of a workspace.
// It divides the tile map into fixed-size cells and lets handleMove broadcast
// only to clients in the 3×3 neighbourhood around the mover, instead of all N-1
// clients in the room.
//
// Thread-safe.  Used only for open-floor movement; room-scoped movement uses
// broadcastToRoom instead (zero overhead — RoomID is already tracked per client).
type AOIGrid struct {
	mu    sync.RWMutex
	cells map[cellKey]map[string]*Client // cellKey → userID → *Client
	pos   map[string]cellKey             // userID → current cell
}

// NewAOIGrid returns a ready-to-use AOIGrid.
func NewAOIGrid() *AOIGrid {
	return &AOIGrid{
		cells: make(map[cellKey]map[string]*Client),
		pos:   make(map[string]cellKey),
	}
}

func tileToCell(tx, ty int) cellKey {
	return cellKey{tx / AOICellSize, ty / AOICellSize}
}

// Move updates the client's cell in the grid.
// A no-op if the client has not crossed a cell boundary.
func (g *AOIGrid) Move(c *Client, tx, ty int) {
	newCell := tileToCell(tx, ty)
	g.mu.Lock()
	defer g.mu.Unlock()
	if old, ok := g.pos[c.UserID]; ok && old == newCell {
		return // still in the same cell — no index update needed
	}
	// remove from old cell
	if old, ok := g.pos[c.UserID]; ok {
		delete(g.cells[old], c.UserID)
		if len(g.cells[old]) == 0 {
			delete(g.cells, old)
		}
	}
	// insert into new cell
	if g.cells[newCell] == nil {
		g.cells[newCell] = make(map[string]*Client)
	}
	g.cells[newCell][c.UserID] = c
	g.pos[c.UserID] = newCell
}

// Remove clears the client from the grid on disconnect.
func (g *AOIGrid) Remove(userID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if old, ok := g.pos[userID]; ok {
		delete(g.cells[old], userID)
		if len(g.cells[old]) == 0 {
			delete(g.cells, old)
		}
	}
	delete(g.pos, userID)
}

// Subscribers returns all clients in the 3×3 cell neighbourhood around (tx, ty),
// excluding the client identified by excludeID.
// At typical office density (1000 CCU / 22500 tiles), this returns ~35 clients.
func (g *AOIGrid) Subscribers(tx, ty int, excludeID string) []*Client {
	center := tileToCell(tx, ty)
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*Client, 0, 64)
	for dy := -1; dy <= 1; dy++ {
		for dx := -1; dx <= 1; dx++ {
			for uid, c := range g.cells[cellKey{center.cx + dx, center.cy + dy}] {
				if uid != excludeID {
					out = append(out, c)
				}
			}
		}
	}
	return out
}
