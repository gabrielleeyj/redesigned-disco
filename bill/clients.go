package bill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encore.dev/storage/sqldb"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

// ClientStatus controls what the caller is allowed to do.
// ACTIVE accounts can read and write bills. SUSPENDED accounts can
// still read (so a frozen account can see its own balance during a
// dispute) but every mutating endpoint rejects them with
// PermissionDenied.
type ClientStatus string

const (
	ClientStatusActive    ClientStatus = "ACTIVE"
	ClientStatusSuspended ClientStatus = "SUSPENDED"
)

// Client is a row from the clients table. The bills service owns this
// table as a stub for an external account / identity service — see
// the migration and auth.go for the production replacement TODO.
type Client struct {
	ID     string
	Name   string
	Status ClientStatus
}

const (
	clientCacheSize    = 1024
	clientCacheTTL     = 5 * time.Minute
	clientLoadTimeout  = 2 * time.Second
)

// clientCache holds per-ID Client values. A 5-minute TTL bounds
// staleness after operator changes (status flips on the clients
// table) without making the cache effectively unbounded. Cache hits
// are the hot path for every authenticated request.
var clientCache = expirable.NewLRU[string, *Client](
	clientCacheSize, nil, clientCacheTTL,
)

// clientLoadGroup deduplicates concurrent DB lookups for the same
// missing client ID so a burst of requests from a new account does
// not stampede the DB.
var clientLoadGroup singleflight.Group

// ErrClientNotFound signals that the asserted X-Account-Id does not
// correspond to a known client. The auth handler maps this to
// Unauthenticated so callers can't probe for valid account IDs by
// watching response codes.
var ErrClientNotFound = errors.New("client not found")

// lookupClient returns the cached Client for id, loading from the DB
// on a miss. Returns ErrClientNotFound for unknown IDs (cached as a
// nil entry to short-circuit repeat probes of bad IDs without paying
// the DB round-trip again). Any other error is an infrastructure
// failure the caller should fail-closed on.
func lookupClient(ctx context.Context, id string) (*Client, error) {
	if c, ok := clientCache.Get(id); ok {
		if c == nil {
			return nil, ErrClientNotFound
		}
		return c, nil
	}

	v, err, _ := clientLoadGroup.Do(id, func() (interface{}, error) {
		if c, ok := clientCache.Get(id); ok {
			return c, nil
		}
		loadCtx, cancel := context.WithTimeout(ctx, clientLoadTimeout)
		defer cancel()

		c, err := loadClientFromDB(loadCtx, id)
		if err != nil {
			if errors.Is(err, sqldb.ErrNoRows) {
				// Cache the miss so repeat probes don't hit the DB.
				clientCache.Add(id, nil)
				return nil, ErrClientNotFound
			}
			return nil, err
		}
		clientCache.Add(id, c)
		return c, nil
	})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrClientNotFound
	}
	return v.(*Client), nil
}

func loadClientFromDB(ctx context.Context, id string) (*Client, error) {
	var c Client
	err := db.QueryRow(ctx, `
		SELECT id, name, status FROM clients WHERE id = $1`, id,
	).Scan(&c.ID, &c.Name, &c.Status)
	if err != nil {
		return nil, fmt.Errorf("load client %s: %w", id, err)
	}
	return &c, nil
}

// invalidateClient drops a single ID from the cache. Used by tests
// and by future admin status-change endpoints so a SUSPEND takes
// effect within the request, not after the TTL.
//
//nolint:unused // used by tests; future admin endpoint will use it
func invalidateClient(id string) {
	clientCache.Remove(id)
}
