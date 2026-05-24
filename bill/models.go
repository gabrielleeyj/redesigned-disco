package bill

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// Currency is an ISO-4217 alphabetic code. Validity is determined at
// runtime by the registry loaded from the currencies table, not by a
// hardcoded enum.
type Currency string

// CurrencyMeta is one row from the currencies table.
type CurrencyMeta struct {
	Code        string
	Name        string
	NumericCode int
	Decimals    int32
}

// Compatibility shims for the original API. New code should use the registry.
const (
	CurrencyUSD Currency = "USD"
	CurrencyGEL Currency = "GEL"
)

// Meta returns the registry entry for c if the currency is supported.
func (c Currency) Meta() (CurrencyMeta, bool) {
	meta, ok := getCurrencies()[c]
	return meta, ok
}

// Valid reports whether c is in the registry.
func (c Currency) Valid() bool {
	_, ok := getCurrencies()[c]
	return ok
}

// Decimals returns the ISO-4217 fractional digit count for the currency.
// Falls back to 2 for unknown codes; callers should also check Valid().
func (c Currency) Decimals() int32 {
	if meta, ok := getCurrencies()[c]; ok {
		return meta.Decimals
	}
	return 2
}

// BillStatus is the lifecycle state of a Bill.
type BillStatus string

const (
	// BillStatusOpen indicates the bill is accepting line items.
	BillStatusOpen BillStatus = "OPEN"
	// BillStatusClosed indicates the bill has been finalised and persisted.
	BillStatusClosed BillStatus = "CLOSED"
)

// CloseReason records why a bill transitioned from OPEN to CLOSED.
// Persisted so audits and downstream consumers can distinguish a
// caller-initiated close from an automatic period-end finalisation.
type CloseReason string

const (
	// CloseReasonSignal indicates the bill was closed by an explicit
	// CloseBill call (caller-initiated).
	CloseReasonSignal CloseReason = "SIGNAL"
	// CloseReasonPeriodEnd indicates the workflow's period-end timer
	// fired and finalised the bill automatically.
	CloseReasonPeriodEnd CloseReason = "PERIOD_END"
)

// Money carries an arbitrary-precision decimal amount tagged with a currency.
// Persistence and arithmetic preserve full precision; rounding is applied at
// display boundaries only.
type Money struct {
	Amount   decimal.Decimal `json:"amount"`
	Currency Currency        `json:"currency"`
}

// NewMoney constructs a Money value.
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

// LineItem is a single charge on a Bill. Amount is in the bill's currency
// at full precision; rounding for display happens at the boundary.
type LineItem struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	Amount      decimal.Decimal `json:"amount"`
	Currency    Currency        `json:"currency"`
	CreatedAt   time.Time       `json:"createdAt"`
}

// Bill is the aggregate root: an open or closed bill with its line items
// and running total. The workflow holds this as in-memory state; the
// activity persists it on close.
type Bill struct {
	ID          string          `json:"id"`
	Status      BillStatus      `json:"status"`
	Currency    Currency        `json:"currency"`
	LineItems   []LineItem      `json:"lineItems"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	CreatedAt   time.Time       `json:"createdAt"`
	ClosedAt    *time.Time      `json:"closedAt,omitempty"`
	PeriodStart *time.Time      `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time      `json:"periodEnd,omitempty"`
	CloseReason CloseReason     `json:"closeReason,omitempty"`
}

// BillWorkflowInput is the argument to BillingWorkflow.
type BillWorkflowInput struct {
	BillID      string     `json:"billId"`
	Currency    Currency   `json:"currency"`
	PeriodStart *time.Time `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time `json:"periodEnd,omitempty"`
	// Snapshot carries accumulated bill state across ContinueAsNew boundaries.
	// Nil for the initial workflow run.
	Snapshot *Bill `json:"snapshot,omitempty"`
}

// BillResult is the value returned by BillingWorkflow on completion.
type BillResult struct {
	BillID      string          `json:"billId"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	Currency    Currency        `json:"currency"`
	ItemCount   int             `json:"itemCount"`
	CloseReason CloseReason     `json:"closeReason"`
}
