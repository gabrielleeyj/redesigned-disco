package bill

import (
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

// TestMain primes the currency cache before any test runs so tests that
// touch Currency.Valid / .Decimals / .Meta (or the AddLineItem endpoint
// validation, which calls them) are hermetic and do not depend on the
// real DB. encore test also runs initService, which would overwrite this
// — that is fine, the DB seed and the test registry agree.
func TestMain(m *testing.M) {
	setCurrencies(testRegistry)
	os.Exit(m.Run())
}
