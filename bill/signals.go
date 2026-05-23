package bill

import "github.com/shopspring/decimal"

const (
	UpdateAddLineItem = "add-line-item"
	SignalCloseBill   = "close-bill"
	QueryBillState    = "bill-state"
)

type AddLineItemInput struct {
	ItemID      string          `json:"itemId"`
	Description string          `json:"description"`
	Amount      decimal.Decimal `json:"amount"`
	Currency    Currency        `json:"currency"`
}

type AddLineItemResult struct {
	ItemID    string          `json:"itemId"`
	BillTotal decimal.Decimal `json:"billTotal"`
	ItemCount int             `json:"itemCount"`
}

type CloseBillSignal struct{}
