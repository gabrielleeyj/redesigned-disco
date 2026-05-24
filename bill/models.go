package bill

import (
	"encoding/json"
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

// BillEventKind tags an entry in the bill_events audit log.
type BillEventKind string

const (
	BillEventOpened    BillEventKind = "OPENED"
	BillEventItemAdded BillEventKind = "ITEM_ADDED"
	BillEventClosed    BillEventKind = "CLOSED"
)

// SystemActor is the actor recorded on events that have no human
// caller — currently only the period-end auto-close fired by the
// workflow timer.
const SystemActor = "system"

// BillEvent is one row from bill_events. The table is append-only at
// the DB layer (triggers block UPDATE/DELETE); the API surface only
// supports reads.
type BillEvent struct {
	ID        string          `json:"id"`
	BillID    string          `json:"billId"`
	Kind      BillEventKind   `json:"kind"`
	Actor     string          `json:"actor"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
}

// FormatAmount renders an amount rounded to the currency's standard
// fractional digits, suffixed with the code (e.g. "10.00 USD",
// "1235 JPY"). Use this for human-facing output only; never for
// further computation.
func (c Currency) FormatAmount(amount decimal.Decimal) string {
	return fmt.Sprintf("%s %s", amount.StringFixed(c.Decimals()), c)
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

// Bill is the aggregate root: an open or closed bill with its running
// total. LineItems is populated by the API layer on read (joined from
// the DB) and is left nil by the workflow's in-memory state. The
// workflow now persists each item incrementally via
// AppendLineItemActivity instead of buffering them in memory until
// close — see docs/architecture.md for the rationale.
type Bill struct {
	ID          string          `json:"id"`
	AccountID   string          `json:"accountId"`
	Status      BillStatus      `json:"status"`
	Currency    Currency        `json:"currency"`
	LineItems   []LineItem      `json:"lineItems,omitempty"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	ItemCount   int             `json:"itemCount"`
	CreatedAt   time.Time       `json:"createdAt"`
	ClosedAt    *time.Time      `json:"closedAt,omitempty"`
	PeriodStart *time.Time      `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time      `json:"periodEnd,omitempty"`
	CloseReason CloseReason     `json:"closeReason,omitempty"`
}

// BillWorkflowInput is the argument to BillingWorkflow.
//
// Snapshot + SeenItemIDs are populated only on ContinueAsNew. They
// carry the running summary (without line items) and the dedup set
// across run boundaries so per-run history stays bounded regardless
// of how many items the bill accumulates.
type BillWorkflowInput struct {
	BillID      string     `json:"billId"`
	AccountID   string     `json:"accountId"`
	Currency    Currency   `json:"currency"`
	PeriodStart *time.Time `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time `json:"periodEnd,omitempty"`
	Snapshot    *Bill      `json:"snapshot,omitempty"`
	SeenItemIDs []string   `json:"seenItemIds,omitempty"`
}

// BillResult is the value returned by BillingWorkflow on completion.
type BillResult struct {
	BillID      string          `json:"billId"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	Currency    Currency        `json:"currency"`
	ItemCount   int             `json:"itemCount"`
	CloseReason CloseReason     `json:"closeReason"`
}
