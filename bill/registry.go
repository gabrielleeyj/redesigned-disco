package bill

import (
	"context"
	"fmt"
	"time"

	"encore.dev/rlog"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

const (
	currenciesCacheKey  = "currencies"
	currenciesCacheTTL  = time.Hour
	currencyLoadTimeout = 5 * time.Second
)

// currencyCache holds the registry under a single logical key. Size = 1
// because there is only one entry; the LRU is here for its TTL-based
// expiry, not its eviction policy. Mirrors the pattern used in
// dtone/nexus admin-api repo/pgdb/transaction/transaction.go.
var currencyCache = expirable.NewLRU[string, map[Currency]CurrencyMeta](
	1, nil, currenciesCacheTTL,
)

// currencyLoadGroup deduplicates concurrent cache-miss reloads so a TTL
// expiry under load triggers exactly one DB round-trip, not N. Without
// this, the read path is vulnerable to a thundering herd.
var currencyLoadGroup singleflight.Group

// getCurrencies returns the registry, refilling from the DB on a cache
// miss (cold start or TTL expiry). Returns nil on DB error; callers
// (Currency.Valid, .Decimals, .Meta) interpret a nil map as "no
// currencies known" — i.e., everything is invalid. That is the
// fail-closed behaviour we want; a transient DB outage should reject
// unknown amounts rather than admit them.
//
// Note: this function is NEVER called from inside a Temporal workflow
// — doing so would break replay determinism. Workflows must validate
// currencies via state captured at workflow start (input.Currency on
// the bill).
func getCurrencies() map[Currency]CurrencyMeta {
	if m, ok := currencyCache.Get(currenciesCacheKey); ok {
		return m
	}

	v, err, _ := currencyLoadGroup.Do(currenciesCacheKey, func() (interface{}, error) {
		if m, ok := currencyCache.Get(currenciesCacheKey); ok {
			return m, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), currencyLoadTimeout)
		defer cancel()
		m, err := loadCurrenciesFromDB(ctx)
		if err != nil {
			return nil, err
		}
		currencyCache.Add(currenciesCacheKey, m)
		return m, nil
	})
	if err != nil {
		rlog.Error("currency cache miss reload failed", "err", err)
		return nil
	}
	if v == nil {
		return nil
	}
	return v.(map[Currency]CurrencyMeta)
}

// setCurrencies primes the cache. Called by initService after the eager
// startup load so the first request does not pay a DB round-trip, and
// usable from tests to inject a controlled registry.
//
//nolint:unused // called by initService, which is invoked by Encore
func setCurrencies(m map[Currency]CurrencyMeta) {
	currencyCache.Add(currenciesCacheKey, m)
}

// refreshCurrencies reloads the registry from the DB and overwrites
// the cache. Returns the loaded entry count for telemetry. Used by
// the admin refresh endpoint to pick up newly inserted currencies
// without waiting for the TTL.
func refreshCurrencies(ctx context.Context) (int, error) {
	m, err := loadCurrenciesFromDB(ctx)
	if err != nil {
		return 0, err
	}
	setCurrencies(m)
	return len(m), nil
}

//nolint:unused // called by initService and via cache miss in getCurrencies
func loadCurrenciesFromDB(ctx context.Context) (map[Currency]CurrencyMeta, error) {
	rows, err := db.Query(ctx, `
		SELECT code, name, numeric_code, minor_unit
		FROM currencies`)
	if err != nil {
		return nil, fmt.Errorf("query currencies: %w", err)
	}
	defer rows.Close()

	out := make(map[Currency]CurrencyMeta)
	for rows.Next() {
		var meta CurrencyMeta
		if err := rows.Scan(&meta.Code, &meta.Name, &meta.NumericCode, &meta.Decimals); err != nil {
			return nil, fmt.Errorf("scan currency: %w", err)
		}
		out[Currency(meta.Code)] = meta
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate currencies: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("currencies table is empty — has the seed migration run?")
	}
	return out, nil
}
