package bill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"encore.dev/rlog"
	"encore.dev/storage/sqldb"
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

// CreateBillActivity inserts the bill row and an OPENED audit event in
// one transaction. Idempotent — a Temporal activity retry, or a
// ContinueAsNew that mistakenly invokes this again, is a no-op rather
// than a constraint failure. The event insert is skipped on the
// no-op path (gated by RETURNING).
func CreateBillActivity(ctx context.Context, bill Bill) error {
	type openedPayload struct {
		Currency    Currency   `json:"currency"`
		PeriodStart *time.Time `json:"periodStart,omitempty"`
		PeriodEnd   *time.Time `json:"periodEnd,omitempty"`
	}
	return inTx(ctx, "create bill", bill.ID, func(tx *sqldb.Tx) error {
		var inserted string
		err := tx.QueryRow(ctx, `
			INSERT INTO bills (id, account_id, status, currency, total_amount, created_at, period_start, period_end)
			VALUES ($1, $2, $3, $4, 0, $5, $6, $7)
			ON CONFLICT (id) DO NOTHING
			RETURNING id`,
			bill.ID, bill.AccountID, string(bill.Status), string(bill.Currency),
			bill.CreatedAt, bill.PeriodStart, bill.PeriodEnd,
		).Scan(&inserted)
		if err != nil {
			if errors.Is(err, sqldb.ErrNoRows) {
				// Row already existed — retry of a successful prior
				// run. Skip the audit event, the original run logged
				// it. Don't return an error: the activity goal is met.
				return nil
			}
			return fmt.Errorf("insert bill: %w", err)
		}

		payload, _ := json.Marshal(openedPayload{
			Currency:    bill.Currency,
			PeriodStart: bill.PeriodStart,
			PeriodEnd:   bill.PeriodEnd,
		})
		if err := appendEvent(ctx, tx, bill.ID, BillEventOpened, bill.AccountID, payload); err != nil {
			return err
		}
		billsOpenedTotal.With(currencyLabels{Currency: string(bill.Currency)}).Increment()
		return nil
	})
}

// AppendLineItemActivity inserts one line item, increments the bill's
// running total, and writes an ITEM_ADDED audit event — all in one
// transaction so they are atomic. Idempotent on item ID: a duplicate
// insert is a no-op, the total is not double-counted, and the event
// is not duplicated either.
func AppendLineItemActivity(ctx context.Context, in AppendLineItemInput) error {
	type itemAddedPayload struct {
		ItemID      string   `json:"itemId"`
		Description string   `json:"description"`
		Amount      string   `json:"amount"`
		Currency    Currency `json:"currency"`
	}
	return inTx(ctx, "append line item", in.BillID, func(tx *sqldb.Tx) error {
		var inserted string
		err := tx.QueryRow(ctx, `
			INSERT INTO line_items (id, bill_id, description, amount, currency, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO NOTHING
			RETURNING id`,
			in.Item.ID, in.BillID, in.Item.Description, in.Item.Amount, string(in.Item.Currency), in.Item.CreatedAt,
		).Scan(&inserted)
		if err != nil {
			if errors.Is(err, sqldb.ErrNoRows) {
				lineItemsAddedTotal.With(lineItemResultLabels{Result: "duplicate"}).Increment()
				return nil // duplicate retry; total and event already recorded
			}
			return fmt.Errorf("insert line item %s: %w", in.Item.ID, err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE bills SET total_amount = total_amount + $1 WHERE id = $2`,
			in.Item.Amount, in.BillID,
		); err != nil {
			return fmt.Errorf("update bill total: %w", err)
		}

		payload, _ := json.Marshal(itemAddedPayload{
			ItemID:      in.Item.ID,
			Description: in.Item.Description,
			Amount:      in.Item.Amount.String(),
			Currency:    in.Item.Currency,
		})
		if err := appendEvent(ctx, tx, in.BillID, BillEventItemAdded, in.Actor, payload); err != nil {
			return err
		}
		lineItemsAddedTotal.With(lineItemResultLabels{Result: "accepted"}).Increment()
		return nil
	})
}

// CloseBillActivity flips the bill to CLOSED, stamps closed_at and the
// close reason, snapshots the authoritative total, and writes the
// CLOSED audit event — atomically. The total is passed in (rather
// than recomputed in SQL) so the workflow's computed total is the
// source of truth.
func CloseBillActivity(ctx context.Context, in CloseBillActivityInput) error {
	type closedPayload struct {
		TotalAmount string      `json:"totalAmount"`
		CloseReason CloseReason `json:"closeReason"`
	}
	return inTx(ctx, "close bill", in.BillID, func(tx *sqldb.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE bills
			SET status = 'CLOSED',
			    total_amount = $2,
			    closed_at = $3,
			    close_reason = $4
			WHERE id = $1`,
			in.BillID, in.TotalAmount, in.ClosedAt, nullableCloseReason(in.CloseReason),
		); err != nil {
			return fmt.Errorf("update bill on close: %w", err)
		}

		payload, _ := json.Marshal(closedPayload{
			TotalAmount: in.TotalAmount.String(),
			CloseReason: in.CloseReason,
		})
		if err := appendEvent(ctx, tx, in.BillID, BillEventClosed, in.Actor, payload); err != nil {
			return err
		}
		billsClosedTotal.With(closeReasonLabels{Reason: string(in.CloseReason)}).Increment()
		return nil
	})
}

// appendEvent writes one row to bill_events inside the caller's
// transaction. Kept private so the activities are the only producers
// of audit entries — no API path can write events.
func appendEvent(ctx context.Context, tx *sqldb.Tx, billID string, kind BillEventKind, actor string, payload []byte) error {
	if actor == "" {
		actor = SystemActor
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO bill_events (bill_id, kind, actor, payload)
		VALUES ($1, $2, $3, $4)`,
		billID, string(kind), actor, payload,
	); err != nil {
		return fmt.Errorf("append %s event: %w", kind, err)
	}
	return nil
}

// inTx runs fn inside a transaction with rollback-on-error and
// rollback-on-panic semantics. Logs rollback failures (rare; usually
// connection-level issues) so they aren't silently lost.
func inTx(ctx context.Context, op, billID string, fn func(*sqldb.Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin tx: %w", op, err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rbErr := tx.Rollback(); rbErr != nil {
			rlog.Error("rollback failed", "op", op, "bill_id", billID, "err", rbErr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: commit: %w", op, err)
	}
	committed = true
	return nil
}

// AppendLineItemInput replaces the prior (item, billID) positional
// signature so the activity can carry the actor (caller account) for
// audit-log attribution without yet another argument.
type AppendLineItemInput struct {
	BillID string
	Actor  string
	Item   LineItem
}

// CloseBillActivityInput carries the fields the close activity needs.
// Actor is the caller's account ID for a signal-driven close, or
// SystemActor for a period-end timer-driven close.
type CloseBillActivityInput struct {
	BillID      string
	Actor       string
	TotalAmount decimal.Decimal
	ClosedAt    time.Time
	CloseReason CloseReason
}
