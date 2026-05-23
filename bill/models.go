package bill

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Currency string

type CurrencyMeta struct {
	Code     string `json:"code"`
	Decimals int32  `json:"decimals"`
	Symbol   string `json:"symbol"`
}

//go:embed currencies.json
var currenciesJSON []byte

// currencies is the registry of supported currencies, populated at init
// from the embedded currencies.json file. Edit that file to add or remove
// currencies; everything else (validation, display, rounding) reads from
// this table.
var currencies = mustLoadCurrencies(currenciesJSON)

// Compatibility shims for the original API. New code should use the registry.
const (
	CurrencyUSD Currency = "USD"
	CurrencyGEL Currency = "GEL"
)

func mustLoadCurrencies(data []byte) map[Currency]CurrencyMeta {
	var entries []CurrencyMeta
	if err := json.Unmarshal(data, &entries); err != nil {
		panic(fmt.Sprintf("bill: parse currencies.json: %v", err))
	}
	if len(entries) == 0 {
		panic("bill: currencies.json is empty")
	}
	out := make(map[Currency]CurrencyMeta, len(entries))
	for i, e := range entries {
		if e.Code == "" {
			panic(fmt.Sprintf("bill: currencies.json entry %d missing code", i))
		}
		if e.Decimals < 0 || e.Decimals > 10 {
			panic(fmt.Sprintf("bill: currencies.json entry %s has invalid decimals %d", e.Code, e.Decimals))
		}
		if _, dup := out[Currency(e.Code)]; dup {
			panic(fmt.Sprintf("bill: duplicate currency %s in currencies.json", e.Code))
		}
		out[Currency(e.Code)] = e
	}
	return out
}

func (c Currency) Meta() (CurrencyMeta, bool) {
	meta, ok := currencies[c]
	return meta, ok
}

func (c Currency) Valid() bool {
	_, ok := currencies[c]
	return ok
}

// Decimals returns the ISO-4217 fractional digit count for the currency.
// Falls back to 2 for unknown codes; callers should also check Valid().
func (c Currency) Decimals() int32 {
	if meta, ok := currencies[c]; ok {
		return meta.Decimals
	}
	return 2
}

type BillStatus string

const (
	BillStatusOpen   BillStatus = "OPEN"
	BillStatusClosed BillStatus = "CLOSED"
)

// Money carries an arbitrary-precision decimal amount tagged with a currency.
// Persistence and arithmetic preserve full precision; rounding is applied at
// display boundaries only.
type Money struct {
	Amount   decimal.Decimal `json:"amount"`
	Currency Currency        `json:"currency"`
}

func NewMoney(amount decimal.Decimal, currency Currency) Money {
	return Money{Amount: amount, Currency: currency}
}

// DisplayAmount renders the value rounded to the currency's standard
// fractional digits. Use this for human-facing output only; never for
// further computation.
func (m Money) DisplayAmount() string {
	decimals := m.Currency.Decimals()
	return fmt.Sprintf("%s %s", m.Amount.StringFixed(decimals), m.Currency)
}

type LineItem struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	Amount      decimal.Decimal `json:"amount"`
	Currency    Currency        `json:"currency"`
	CreatedAt   time.Time       `json:"createdAt"`
}

type Bill struct {
	ID          string          `json:"id"`
	Status      BillStatus      `json:"status"`
	Currency    Currency        `json:"currency"`
	LineItems   []LineItem      `json:"lineItems"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	CreatedAt   time.Time       `json:"createdAt"`
	ClosedAt    *time.Time      `json:"closedAt,omitempty"`
}

type BillWorkflowInput struct {
	BillID   string   `json:"billId"`
	Currency Currency `json:"currency"`
	// Snapshot carries accumulated bill state across ContinueAsNew boundaries.
	// Nil for the initial workflow run.
	Snapshot *Bill `json:"snapshot,omitempty"`
}

type BillResult struct {
	BillID      string          `json:"billId"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	Currency    Currency        `json:"currency"`
	ItemCount   int             `json:"itemCount"`
}
