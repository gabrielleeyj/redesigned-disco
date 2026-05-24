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
//
// CallerAccountID is the asserted identity from the auth handler. The
// validator rejects the update if it does not match the bill's owner;
// callers must NOT set this from user input. The API layer is the only
// trusted source for this field.
type AddLineItemInput struct {
	ItemID          string          `json:"itemId"`
	Description     string          `json:"description"`
	Amount          decimal.Decimal `json:"amount"`
	Currency        Currency        `json:"currency"`
	CallerAccountID string          `json:"callerAccountId"`
}

// AddLineItemResult is the return value of the AddLineItem update.
type AddLineItemResult struct {
	ItemID    string          `json:"itemId"`
	BillTotal decimal.Decimal `json:"billTotal"`
	ItemCount int             `json:"itemCount"`
}

// CloseBillSignal is the payload of the CloseBill signal (empty by design).
type CloseBillSignal struct{}
