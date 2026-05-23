package bill

import "github.com/shopspring/decimal"

// Names of the Temporal update, signal, and query the bill workflow exposes.
const (
	// UpdateAddLineItem is the synchronous workflow update used to add a
	// line item. The update returns AddLineItemResult on success and an
	// ApplicationError when the validator rejects the input.
	UpdateAddLineItem = "add-line-item"
	// SignalCloseBill triggers the workflow to drain pending updates,
	// persist the bill, and complete.
	SignalCloseBill = "close-bill"
	// QueryBillState returns the current in-memory Bill from the workflow.
	QueryBillState = "bill-state"
)

// AddLineItemInput is the payload of the AddLineItem update.
type AddLineItemInput struct {
	ItemID      string          `json:"itemId"`
	Description string          `json:"description"`
	Amount      decimal.Decimal `json:"amount"`
	Currency    Currency        `json:"currency"`
}

// AddLineItemResult is the return value of the AddLineItem update.
type AddLineItemResult struct {
	ItemID    string          `json:"itemId"`
	BillTotal decimal.Decimal `json:"billTotal"`
	ItemCount int             `json:"itemCount"`
}

// CloseBillSignal is the payload of the CloseBill signal (empty by design).
type CloseBillSignal struct{}
