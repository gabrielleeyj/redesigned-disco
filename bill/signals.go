package bill

const (
	UpdateAddLineItem = "add-line-item"
	SignalCloseBill   = "close-bill"
	QueryBillState    = "bill-state"
)

type AddLineItemInput struct {
	ItemID      string   `json:"itemId"`
	Description string   `json:"description"`
	AmountMinor int64    `json:"amountMinor"`
	Currency    Currency `json:"currency"`
}

type AddLineItemResult struct {
	ItemID    string `json:"itemId"`
	BillTotal int64  `json:"billTotal"`
	ItemCount int    `json:"itemCount"`
}

type CloseBillSignal struct{}
