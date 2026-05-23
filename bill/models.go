package bill

import (
	"fmt"
	"time"
)

type Currency string

const (
	CurrencyUSD Currency = "USD"
	CurrencyGEL Currency = "GEL"
)

func (c Currency) Valid() bool {
	return c == CurrencyUSD || c == CurrencyGEL
}

func (c Currency) MinorUnitFactor() int64 {
	return 100
}

type BillStatus string

const (
	BillStatusOpen   BillStatus = "OPEN"
	BillStatusClosed BillStatus = "CLOSED"
)

type Money struct {
	Amount   int64    `json:"amount"`
	Currency Currency `json:"currency"`
}

func (m Money) DisplayAmount() string {
	whole := m.Amount / m.Currency.MinorUnitFactor()
	frac := m.Amount % m.Currency.MinorUnitFactor()
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("%d.%02d %s", whole, frac, m.Currency)
}

type LineItem struct {
	ID          string    `json:"id"`
	Description string    `json:"description"`
	Amount      Money     `json:"amount"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Bill struct {
	ID          string     `json:"id"`
	Status      BillStatus `json:"status"`
	Currency    Currency   `json:"currency"`
	LineItems   []LineItem `json:"lineItems"`
	TotalAmount int64      `json:"totalAmount"`
	CreatedAt   time.Time  `json:"createdAt"`
	ClosedAt    *time.Time `json:"closedAt,omitempty"`
}

type BillWorkflowInput struct {
	BillID   string   `json:"billId"`
	Currency Currency `json:"currency"`
	// Snapshot carries accumulated bill state across ContinueAsNew boundaries.
	// Nil for the initial workflow run.
	Snapshot *Bill `json:"snapshot,omitempty"`
}

type BillResult struct {
	BillID      string   `json:"billId"`
	TotalAmount int64    `json:"totalAmount"`
	Currency    Currency `json:"currency"`
	ItemCount   int      `json:"itemCount"`
}
