package bill

import (
	"context"
	"os"
	"testing"
)

// testRegistry mirrors the rows seeded by migration 2_currencies.up.sql.
// Kept in sync by convention; if the migration grows, update this map too.
var testRegistry = map[Currency]CurrencyMeta{
	"USD": {Code: "USD", Name: "US Dollar", NumericCode: 840, Decimals: 2},
	"EUR": {Code: "EUR", Name: "Euro", NumericCode: 978, Decimals: 2},
	"GBP": {Code: "GBP", Name: "Pound Sterling", NumericCode: 826, Decimals: 2},
	"GEL": {Code: "GEL", Name: "Lari", NumericCode: 981, Decimals: 2},
	"JPY": {Code: "JPY", Name: "Yen", NumericCode: 392, Decimals: 0},
	"KRW": {Code: "KRW", Name: "Won", NumericCode: 410, Decimals: 0},
	"BHD": {Code: "BHD", Name: "Bahraini Dinar", NumericCode: 48, Decimals: 3},
	"KWD": {Code: "KWD", Name: "Kuwaiti Dinar", NumericCode: 414, Decimals: 3},
}

// testAccountIDs are seeded into the clients table by TestMain so the
// bills.account_id FK is satisfied when tests INSERT bills directly
// with these IDs. Tests that need a different account (e.g. the
// "other-account" leak-check) add it here too.
var testAccountIDs = []struct {
	id, name string
	status   ClientStatus
}{
	{id: "acct-test", name: "Test Account", status: ClientStatusActive},
	{id: "acct-wf-test", name: "Workflow Test Account", status: ClientStatusActive},
	{id: "other-account", name: "Other Account", status: ClientStatusActive},
}

// TestMain primes the currency cache and seeds test clients before
// any test runs so tests that touch Currency.Valid / .Decimals /
// .Meta (or the AddLineItem endpoint validation, which calls them)
// are hermetic and do not depend on the real DB seed alone. encore
// test also runs initService, which would overwrite the currency
// cache — that is fine, the DB seed and the test registry agree.
//
// Client seeding uses INSERT ... ON CONFLICT so repeated test runs
// against the same DB are idempotent.
func TestMain(m *testing.M) {
	setCurrencies(testRegistry)
	seedTestClients()
	os.Exit(m.Run())
}

func seedTestClients() {
	ctx := context.Background()
	for _, c := range testAccountIDs {
		_, err := db.Exec(ctx, `
			INSERT INTO clients (id, name, status) VALUES ($1, $2, $3)
			ON CONFLICT (id) DO NOTHING`,
			c.id, c.name, string(c.status))
		if err != nil {
			panic("seed test client " + c.id + ": " + err.Error())
		}
	}
}
