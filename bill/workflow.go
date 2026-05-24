package bill

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// continueAsNewThreshold caps the number of line items per workflow run.
// Once the limit is hit, the workflow continues-as-new with a snapshot of
// state, keeping per-run history bounded.
const continueAsNewThreshold = 1000

// BillingWorkflow is the Temporal workflow that backs a bill's lifecycle.
// It accumulates line items via the AddLineItem update, completes on the
// CloseBill signal, and persists the final state via PersistBillActivity.
//
// Replay-determinism note: this workflow MUST NOT call getCurrencies() or
// touch any other runtime-mutable state. Currency validation is the
// endpoint's responsibility; the workflow only enforces that incoming
// items match the bill's locked-in currency (captured at start).
func BillingWorkflow(ctx workflow.Context, input BillWorkflowInput) (BillResult, error) {
	bill := initialBill(ctx, input)
	seen := make(map[string]struct{}, len(bill.LineItems))
	for _, item := range bill.LineItems {
		seen[item.ID] = struct{}{}
	}

	if err := workflow.SetQueryHandler(ctx, QueryBillState, func() (Bill, error) {
		return bill, nil
	}); err != nil {
		return BillResult{}, err
	}

	err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		UpdateAddLineItem,
		func(ctx workflow.Context, in AddLineItemInput) (AddLineItemResult, error) {
			handleAddLineItem(&bill, seen, in, workflow.Now(ctx))
			return AddLineItemResult{
				ItemID:    in.ItemID,
				BillTotal: bill.TotalAmount,
				ItemCount: len(bill.LineItems),
			}, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, in AddLineItemInput) error {
				if in.Currency != bill.Currency {
					return fmt.Errorf("currency mismatch: bill is %s, item is %s", bill.Currency, in.Currency)
				}
				if !in.Amount.IsPositive() {
					return fmt.Errorf("amount must be positive")
				}
				if in.Description == "" {
					return fmt.Errorf("description is required")
				}
				return nil
			},
		},
	)
	if err != nil {
		return BillResult{}, err
	}

	closeCh := workflow.GetSignalChannel(ctx, SignalCloseBill)

	closed := false
	workflow.Go(ctx, func(ctx workflow.Context) {
		var signal CloseBillSignal
		closeCh.Receive(ctx, &signal)
		closed = true
	})

	// Wake on either a close signal OR the line-item threshold so a long
	// stream of updates can trigger continue-as-new without waiting for a
	// close. Update handlers run on independent coroutines, so a plain
	// selector waiting on the close channel would never observe item
	// growth between selects.
	if err := workflow.Await(ctx, func() bool {
		return closed || len(bill.LineItems) >= continueAsNewThreshold
	}); err != nil {
		return BillResult{}, fmt.Errorf("await close or threshold: %w", err)
	}

	// Drain any updates accepted before we decided to close or roll over.
	// AllHandlersFinished ensures their state mutations land in `bill`
	// before we snapshot or persist.
	if err := workflow.Await(ctx, func() bool {
		return workflow.AllHandlersFinished(ctx)
	}); err != nil {
		return BillResult{}, fmt.Errorf("await pending updates: %w", err)
	}

	if !closed {
		// Deep-copy LineItems so the new workflow run owns an independent
		// backing array. A shallow struct copy aliases the slice, and any
		// future append in the continued run could write into our snapshot.
		snapshot := bill
		snapshot.LineItems = append([]LineItem(nil), bill.LineItems...)
		return BillResult{}, workflow.NewContinueAsNewError(ctx, BillingWorkflow, BillWorkflowInput{
			BillID:   input.BillID,
			Currency: input.Currency,
			Snapshot: &snapshot,
		})
	}

	now := workflow.Now(ctx)
	bill.Status = BillStatusClosed
	bill.ClosedAt = &now

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	})
	if err := workflow.ExecuteActivity(activityCtx, PersistBillActivity, bill).Get(ctx, nil); err != nil {
		return BillResult{}, err
	}

	return BillResult{
		BillID:      bill.ID,
		TotalAmount: bill.TotalAmount,
		Currency:    bill.Currency,
		ItemCount:   len(bill.LineItems),
	}, nil
}

func initialBill(ctx workflow.Context, input BillWorkflowInput) Bill {
	if input.Snapshot != nil {
		return *input.Snapshot
	}
	return Bill{
		ID:        input.BillID,
		Status:    BillStatusOpen,
		Currency:  input.Currency,
		LineItems: []LineItem{},
		CreatedAt: workflow.Now(ctx),
	}
}

func handleAddLineItem(bill *Bill, seen map[string]struct{}, in AddLineItemInput, now time.Time) {
	if _, dup := seen[in.ItemID]; dup {
		return
	}
	bill.LineItems = append(bill.LineItems, LineItem{
		ID:          in.ItemID,
		Description: in.Description,
		Amount:      in.Amount,
		Currency:    in.Currency,
		CreatedAt:   now,
	})
	bill.TotalAmount = bill.TotalAmount.Add(in.Amount)
	seen[in.ItemID] = struct{}{}
}
