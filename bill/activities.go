package bill

import (
	"context"
	"fmt"

	"encore.dev/rlog"
)

func PersistBillActivity(ctx context.Context, bill Bill) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rbErr := tx.Rollback(); rbErr != nil {
			rlog.Error("rollback failed", "bill_id", bill.ID, "err", rbErr)
		}
	}()

	_, err = tx.Exec(ctx, `
		INSERT INTO bills (id, status, currency, total_amount, created_at, closed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			total_amount = EXCLUDED.total_amount,
			closed_at = EXCLUDED.closed_at`,
		bill.ID, string(bill.Status), string(bill.Currency), bill.TotalAmount, bill.CreatedAt, bill.ClosedAt,
	)
	if err != nil {
		return fmt.Errorf("insert bill: %w", err)
	}

	for _, item := range bill.LineItems {
		_, err = tx.Exec(ctx, `
			INSERT INTO line_items (id, bill_id, description, amount_minor, currency, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO NOTHING`,
			item.ID, bill.ID, item.Description, item.Amount.Amount, string(item.Amount.Currency), item.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert line item %s: %w", item.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return nil
}
