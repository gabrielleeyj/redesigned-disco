package bill

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"encore.dev/rlog"
	"github.com/hashicorp/golang-lru/v2/expirable"
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

// getCurrencies returns the registry, refilling from the DB on a cache
// miss (cold start or TTL expiry). Returns nil on DB error; callers
// (Currency.Valid, .Decimals, .Meta) interpret a nil map as "no
// currencies known" — i.e., everything is invalid. That is the
// fail-closed behaviour we want; a transient DB outage should reject
// unknown amounts rather than admit them.
func getCurrencies() map[Currency]CurrencyMeta {
	if m, ok := currencyCache.Get(currenciesCacheKey); ok {
		return m
	}
	ctx, cancel := context.WithTimeout(context.Background(), currencyLoadTimeout)
	defer cancel()
	m, err := loadCurrenciesFromDB(ctx)
	if err != nil {
		rlog.Error("currency cache miss reload failed", "err", err)
		return nil
	}
	currencyCache.Add(currenciesCacheKey, m)
	return m
}

// setCurrencies primes the cache. Called by initService after the eager
// startup load so the first request does not pay a DB round-trip, and
// usable from tests to inject a controlled registry.
//
//nolint:unused // called by initService, which is invoked by Encore
func setCurrencies(m map[Currency]CurrencyMeta) {
	currencyCache.Add(currenciesCacheKey, m)
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
		var (
			meta        CurrencyMeta
			numericCode sql.NullInt32
		)
		if err := rows.Scan(&meta.Code, &meta.Name, &numericCode, &meta.Decimals); err != nil {
			return nil, fmt.Errorf("scan currency: %w", err)
		}
		if numericCode.Valid {
			meta.NumericCode = int(numericCode.Int32)
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
