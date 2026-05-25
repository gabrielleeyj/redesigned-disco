package bill

import (
	"encore.dev/beta/errs"
	"github.com/shopspring/decimal"
)

// API-boundary view types. The internal models (Bill, LineItem) carry
// shopspring/decimal.Decimal values; that type marshals JSON as a
// string but Encore's schema generator introspects it via Go
// reflection and exposes it as a nested object in the dev dashboard.
// The view types below replace decimal fields with `string` so the
// generated schema matches the actual wire shape.
//
// Internal types stay on decimal.Decimal for full-precision
// arithmetic, DB scanning, and Temporal payloads. Conversion happens
// only at the HTTP boundary.

// BillView is the API-facing shape of a Bill.
type BillView struct {
	ID          string         `json:"id"`
	AccountID   string         `json:"accountId"`
	Status      BillStatus     `json:"status"`
	Currency    Currency       `json:"currency"`
	LineItems   []LineItemView `json:"lineItems,omitempty"`
	TotalAmount string         `json:"totalAmount"`
	ItemCount   int            `json:"itemCount"`
	CreatedAt   string         `json:"createdAt"`
	ClosedAt    *string        `json:"closedAt,omitempty"`
	PeriodStart *string        `json:"periodStart,omitempty"`
	PeriodEnd   *string        `json:"periodEnd,omitempty"`
	CloseReason CloseReason    `json:"closeReason,omitempty"`
}

// LineItemView is the API-facing shape of a LineItem.
type LineItemView struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	Amount      string   `json:"amount"`
	Currency    Currency `json:"currency"`
	CreatedAt   string   `json:"createdAt"`
}

func toBillView(b Bill) BillView {
	v := BillView{
		ID:          b.ID,
		AccountID:   b.AccountID,
		Status:      b.Status,
		Currency:    b.Currency,
		TotalAmount: b.TotalAmount.String(),
		ItemCount:   b.ItemCount,
		CreatedAt:   b.CreatedAt.Format(rfc3339Nano),
		CloseReason: b.CloseReason,
	}
	if b.ClosedAt != nil {
		s := b.ClosedAt.Format(rfc3339Nano)
		v.ClosedAt = &s
	}
	if b.PeriodStart != nil {
		s := b.PeriodStart.Format(rfc3339Nano)
		v.PeriodStart = &s
	}
	if b.PeriodEnd != nil {
		s := b.PeriodEnd.Format(rfc3339Nano)
		v.PeriodEnd = &s
	}
	if b.LineItems != nil {
		v.LineItems = toLineItemViews(b.LineItems)
	}
	return v
}

func toLineItemView(i LineItem) LineItemView {
	return LineItemView{
		ID:          i.ID,
		Description: i.Description,
		Amount:      i.Amount.String(),
		Currency:    i.Currency,
		CreatedAt:   i.CreatedAt.Format(rfc3339Nano),
	}
}

func toLineItemViews(items []LineItem) []LineItemView {
	out := make([]LineItemView, len(items))
	for i, it := range items {
		out[i] = toLineItemView(it)
	}
	return out
}

// parseAmount turns a wire-format amount string into a decimal. The
// validator enforces positivity (the workflow validator does the same
// check, but failing fast here means we don't pay a Temporal
// round-trip on bad input). Returns an errs.Error so callers can
// return it directly.
func parseAmount(s string) (decimal.Decimal, *errs.Error) {
	if s == "" {
		return decimal.Decimal{}, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "amount is required",
		}
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "amount must be a decimal string (e.g. \"15.99\")",
		}
	}
	if !d.IsPositive() {
		return decimal.Decimal{}, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "amount must be positive",
		}
	}
	return d, nil
}

// rfc3339Nano is the timestamp format used in API responses.
// Using a const here so all response timestamps stay consistent;
// time.Time would marshal with the same format via its own
// MarshalJSON, but we're stringifying explicitly to keep the schema
// uniform.
const rfc3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
