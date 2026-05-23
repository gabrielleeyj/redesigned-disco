package bill

const (
	SignalAddLineItem = "add-line-item"
	SignalCloseBill   = "close-bill"
	QueryBillState    = "bill-state"
)

type AddLineItemSignal struct {
	ItemID      string   `json:"itemId"`
	Description string   `json:"description"`
	AmountMinor int64    `json:"amountMinor"`
	Currency    Currency `json:"currency"`
}

type CloseBillSignal struct{}
