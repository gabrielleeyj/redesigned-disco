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

// currencyMismatchErrType is the ApplicationError type used by the
// AddLineItem validator when an item's currency does not match the
// bill's. Surfaced to the API layer so the endpoint can return a
// FailedPrecondition (409) instead of a generic InvalidArgument.
const currencyMismatchErrType = "CurrencyMismatch"

// BillingWorkflow is the Temporal workflow that backs a bill's lifecycle.
// It accumulates line items via the AddLineItem update, completes on the
// CloseBill signal or when the period-end timer fires, and persists the
// final state via PersistBillActivity.
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

	if err := registerAddLineItemHandler(ctx, &bill, seen); err != nil {
		return BillResult{}, err
	}

	closeReason, err := awaitCloseOrThreshold(ctx, &bill, input.PeriodEnd)
	if err != nil {
		return BillResult{}, err
	}

	// Drain any updates accepted before we decided to close or roll over.
	// AllHandlersFinished ensures their state mutations land in `bill`
	// before we snapshot or persist.
	if err := workflow.Await(ctx, func() bool {
		return workflow.AllHandlersFinished(ctx)
	}); err != nil {
		return BillResult{}, fmt.Errorf("await pending updates: %w", err)
	}

	if closeReason == "" {
		return BillResult{}, continueAsNew(ctx, input, bill)
	}

	return finalizeAndPersist(ctx, &bill, closeReason)
}

// awaitCloseOrThreshold blocks until one of: the CloseBill signal is
// received, the period-end timer fires, or the per-run line-item
// threshold is reached (triggering ContinueAsNew). Returns the close
// reason on signal/timer, or "" when the threshold fired.
func awaitCloseOrThreshold(ctx workflow.Context, bill *Bill, periodEnd *time.Time) (CloseReason, error) {
	closeCh := workflow.GetSignalChannel(ctx, SignalCloseBill)

	var reason CloseReason
	settled := false

	// Drive the signal + timer branches from a dedicated coroutine so the
	// outer Await can also observe the threshold growing via update
	// handlers running on independent coroutines.
	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(closeCh, func(c workflow.ReceiveChannel, _ bool) {
			var sig CloseBillSignal
			c.Receive(ctx, &sig)
			reason = CloseReasonSignal
			settled = true
		})
		if periodEnd != nil {
			delay := periodEnd.Sub(workflow.Now(ctx))
			if delay <= 0 {
				// Period is already over (e.g. ContinueAsNew of a long
				// bill, or a backdated period). Fire immediately rather
				// than scheduling a zero/negative timer.
				reason = CloseReasonPeriodEnd
				settled = true
				return
			}
			selector.AddFuture(workflow.NewTimer(ctx, delay), func(workflow.Future) {
				reason = CloseReasonPeriodEnd
				settled = true
			})
		}
		selector.Select(ctx)
	})

	if err := workflow.Await(ctx, func() bool {
		return settled || len(bill.LineItems) >= continueAsNewThreshold
	}); err != nil {
		return "", fmt.Errorf("await close or threshold: %w", err)
	}
	return reason, nil
}

func registerAddLineItemHandler(ctx workflow.Context, bill *Bill, seen map[string]struct{}) error {
	return workflow.SetUpdateHandlerWithOptions(
		ctx,
		UpdateAddLineItem,
		func(ctx workflow.Context, in AddLineItemInput) (AddLineItemResult, error) {
			handleAddLineItem(bill, seen, in, workflow.Now(ctx))
			return AddLineItemResult{
				ItemID:    in.ItemID,
				BillTotal: bill.TotalAmount,
				ItemCount: len(bill.LineItems),
			}, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, in AddLineItemInput) error {
				if in.Currency != bill.Currency {
					return temporal.NewApplicationError(
						fmt.Sprintf("currency mismatch: bill is %s, item is %s", bill.Currency, in.Currency),
						currencyMismatchErrType,
					)
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
}

func continueAsNew(ctx workflow.Context, input BillWorkflowInput, bill Bill) error {
	// Deep-copy LineItems so the new workflow run owns an independent
	// backing array. A shallow struct copy aliases the slice, and any
	// future append in the continued run could write into our snapshot.
	snapshot := bill
	snapshot.LineItems = append([]LineItem(nil), bill.LineItems...)
	return workflow.NewContinueAsNewError(ctx, BillingWorkflow, BillWorkflowInput{
		BillID:      input.BillID,
		Currency:    input.Currency,
		PeriodStart: input.PeriodStart,
		PeriodEnd:   input.PeriodEnd,
		Snapshot:    &snapshot,
	})
}

func finalizeAndPersist(ctx workflow.Context, bill *Bill, reason CloseReason) (BillResult, error) {
	now := workflow.Now(ctx)
	bill.Status = BillStatusClosed
	bill.ClosedAt = &now
	bill.CloseReason = reason

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
	if err := workflow.ExecuteActivity(activityCtx, PersistBillActivity, *bill).Get(ctx, nil); err != nil {
		return BillResult{}, err
	}

	return BillResult{
		BillID:      bill.ID,
		TotalAmount: bill.TotalAmount,
		Currency:    bill.Currency,
		ItemCount:   len(bill.LineItems),
		CloseReason: reason,
	}, nil
}

func initialBill(ctx workflow.Context, input BillWorkflowInput) Bill {
	if input.Snapshot != nil {
		return *input.Snapshot
	}
	return Bill{
		ID:          input.BillID,
		Status:      BillStatusOpen,
		Currency:    input.Currency,
		LineItems:   []LineItem{},
		CreatedAt:   workflow.Now(ctx),
		PeriodStart: input.PeriodStart,
		PeriodEnd:   input.PeriodEnd,
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
