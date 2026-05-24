package bill

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// nullableCloseReason converts the CloseReason enum into a value
// suitable for INSERT/UPDATE — empty string becomes SQL NULL so the
// column's CHECK constraint isn't violated on intermediate writes.
func nullableCloseReason(r CloseReason) interface{} {
	if r == "" {
		return nil
	}
	return string(r)
}

// CreateBillActivity inserts the bill row at workflow start. Idempotent
// — a Temporal activity retry, or a ContinueAsNew that mistakenly calls
// this again, is a no-op rather than a constraint failure.
//
// Open bills are now visible in the DB from creation; this is the
// inversion of the original "no DB writes until close" design. The
// reason for the change is scale: a workflow that accumulates line
// items in memory for an entire billing period either grows its
// history unboundedly or pays a giant ContinueAsNew snapshot cost.
// Persisting incrementally bounds workflow history and makes open
// bills observable to BI / ops.
func CreateBillActivity(ctx context.Context, bill Bill) error {
	_, err := db.Exec(ctx, `
		INSERT INTO bills (id, account_id, status, currency, total_amount, created_at, period_start, period_end)
		VALUES ($1, $2, $3, $4, 0, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING`,
		bill.ID, bill.AccountID, string(bill.Status), string(bill.Currency),
		bill.CreatedAt, bill.PeriodStart, bill.PeriodEnd,
	)
	if err != nil {
		return fmt.Errorf("insert bill: %w", err)
	}
	return nil
}

// AppendLineItemActivity inserts one line item and increments the
// bill's running total atomically. Idempotent on item ID: a duplicate
// insert is a no-op AND the total is not double-counted, because the
// UPDATE is gated on the INSERT actually producing a row.
//
// The workflow's seen-set normally prevents duplicates from being
// dispatched at all; this guard handles the activity-retry case where
// the first attempt succeeded at the DB but failed to report success
// back to the workflow.
func AppendLineItemActivity(ctx context.Context, item LineItem, billID string) error {
	_, err := db.Exec(ctx, `
		WITH ins AS (
			INSERT INTO line_items (id, bill_id, description, amount, currency, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO NOTHING
			RETURNING id
		)
		UPDATE bills
		SET total_amount = total_amount + $4
		WHERE id = $2 AND EXISTS (SELECT 1 FROM ins)`,
		item.ID, billID, item.Description, item.Amount, string(item.Currency), item.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("append line item %s: %w", item.ID, err)
	}
	return nil
}

// CloseBillActivity flips the bill to CLOSED, stamps closed_at and the
// close reason, and snapshots the authoritative total. The total is
// passed in (rather than recomputed in SQL) so the workflow's
// computed total is the source of truth — defends against partial
// retries of AppendLineItemActivity leaving the in-row total slightly
// behind.
func CloseBillActivity(ctx context.Context, in CloseBillActivityInput) error {
	_, err := db.Exec(ctx, `
		UPDATE bills
		SET status = 'CLOSED',
		    total_amount = $2,
		    closed_at = $3,
		    close_reason = $4
		WHERE id = $1`,
		in.BillID, in.TotalAmount, in.ClosedAt, nullableCloseReason(in.CloseReason),
	)
	if err != nil {
		return fmt.Errorf("close bill %s: %w", in.BillID, err)
	}
	return nil
}

// CloseBillActivityInput carries the fields the close activity needs.
// Defined as a struct so future fields (e.g. final invoice URL) can be
// added without changing the activity signature.
type CloseBillActivityInput struct {
	BillID      string
	TotalAmount decimal.Decimal
	ClosedAt    time.Time
	CloseReason CloseReason
}
