package bill

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// continueAsNewThreshold caps the number of line items processed per
// workflow run. Once the limit is hit, the workflow continues-as-new
// with a small handoff (running total + seen-set) so per-run history
// stays bounded. Line items themselves live in the DB; the new run
// does not carry them.
const continueAsNewThreshold = 1000

// currencyMismatchErrType is the ApplicationError type used by the
// AddLineItem validator when an item's currency does not match the
// bill's. Surfaced to the API layer so the endpoint can return a
// FailedPrecondition (409) instead of a generic InvalidArgument.
const currencyMismatchErrType = "CurrencyMismatch"

// billNotFoundErrType is the ApplicationError type used by the
// AddLineItem validator when the caller's account does not own the
// bill. Surfaced as 404 NotFound at the API boundary so existence is
// not leaked to non-owners.
const billNotFoundErrType = "BillNotFound"

// shortActivityOpts applies to the fast DB activities (create row,
// append item, close row). Each is a single small Postgres write;
// generous retries handle transient pool exhaustion or restart blips.
func shortActivityOpts() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    15 * time.Second,
		ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    5,
		},
	}
}

// BillingWorkflow is the Temporal workflow that backs a bill's
// lifecycle. It accumulates a running total via the AddLineItem
// update (persisting each item via an activity), completes on the
// CloseBill signal or when the period-end timer fires, and flips the
// persisted row to CLOSED via CloseBillActivity.
//
// Replay-determinism note: this workflow MUST NOT call getCurrencies()
// or touch any other runtime-mutable state. Currency validation is
// the endpoint's responsibility; the workflow only enforces that
// incoming items match the bill's locked-in currency (captured at
// start).
func BillingWorkflow(ctx workflow.Context, input BillWorkflowInput) (BillResult, error) {
	bill := initialBill(ctx, input)
	seen := initialSeen(input)

	if input.Snapshot == nil {
		// First run — persist the bill row. ContinueAsNew runs skip
		// this; the row was already inserted by the original run.
		actCtx := workflow.WithActivityOptions(ctx, shortActivityOpts())
		if err := workflow.ExecuteActivity(actCtx, CreateBillActivity, bill).Get(ctx, nil); err != nil {
			return BillResult{}, fmt.Errorf("persist bill on create: %w", err)
		}
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

	// Drain any updates accepted before we decided to close or roll
	// over. AllHandlersFinished ensures their state mutations land in
	// `bill` (and their DB writes complete) before we close or hand
	// off.
	if err := workflow.Await(ctx, func() bool {
		return workflow.AllHandlersFinished(ctx)
	}); err != nil {
		return BillResult{}, fmt.Errorf("await pending updates: %w", err)
	}

	if closeReason == "" {
		return BillResult{}, continueAsNew(ctx, input, bill, seen)
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

	// Drive the signal + timer branches from a dedicated coroutine so
	// the outer Await can also observe the threshold growing via
	// update handlers running on independent coroutines.
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
		return settled || bill.ItemCount >= continueAsNewThreshold
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
			// Duplicate item ID — return the prior result shape
			// without re-issuing the activity or mutating state.
			if _, dup := seen[in.ItemID]; dup {
				return AddLineItemResult{
					ItemID:    in.ItemID,
					BillTotal: bill.TotalAmount,
					ItemCount: bill.ItemCount,
				}, nil
			}

			item := LineItem{
				ID:          in.ItemID,
				Description: in.Description,
				Amount:      in.Amount,
				Currency:    in.Currency,
				CreatedAt:   workflow.Now(ctx),
			}

			// Persist before we acknowledge to the caller — by the
			// time UpdateWorkflow returns, the item is durable. If
			// the activity fails after exhausting retries the update
			// returns the error and the caller can retry; the seen
			// set is NOT advanced, so the retry will reattempt the
			// insert (idempotent via ON CONFLICT).
			actCtx := workflow.WithActivityOptions(ctx, shortActivityOpts())
			if err := workflow.ExecuteActivity(actCtx, AppendLineItemActivity, item, bill.ID).Get(ctx, nil); err != nil {
				return AddLineItemResult{}, fmt.Errorf("persist line item: %w", err)
			}

			bill.TotalAmount = bill.TotalAmount.Add(in.Amount)
			bill.ItemCount++
			seen[in.ItemID] = struct{}{}

			return AddLineItemResult{
				ItemID:    in.ItemID,
				BillTotal: bill.TotalAmount,
				ItemCount: bill.ItemCount,
			}, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, in AddLineItemInput) error {
				if in.CallerAccountID != bill.AccountID {
					return temporal.NewApplicationError(
						"bill not found",
						billNotFoundErrType,
					)
				}
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

func continueAsNew(ctx workflow.Context, input BillWorkflowInput, bill Bill, seen map[string]struct{}) error {
	// Hand off the running summary + dedup set ONLY. Line items live
	// in the DB; the new run loads them on query. This keeps the
	// ContinueAsNew payload constant regardless of bill size.
	seenIDs := make([]string, 0, len(seen))
	for id := range seen {
		seenIDs = append(seenIDs, id)
	}

	snapshot := Bill{
		ID:          bill.ID,
		AccountID:   bill.AccountID,
		Status:      bill.Status,
		Currency:    bill.Currency,
		TotalAmount: bill.TotalAmount,
		ItemCount:   bill.ItemCount,
		CreatedAt:   bill.CreatedAt,
		PeriodStart: bill.PeriodStart,
		PeriodEnd:   bill.PeriodEnd,
	}

	return workflow.NewContinueAsNewError(ctx, BillingWorkflow, BillWorkflowInput{
		BillID:      input.BillID,
		AccountID:   input.AccountID,
		Currency:    input.Currency,
		PeriodStart: input.PeriodStart,
		PeriodEnd:   input.PeriodEnd,
		Snapshot:    &snapshot,
		SeenItemIDs: seenIDs,
	})
}

func finalizeAndPersist(ctx workflow.Context, bill *Bill, reason CloseReason) (BillResult, error) {
	now := workflow.Now(ctx)
	bill.Status = BillStatusClosed
	bill.ClosedAt = &now
	bill.CloseReason = reason

	actCtx := workflow.WithActivityOptions(ctx, shortActivityOpts())
	err := workflow.ExecuteActivity(actCtx, CloseBillActivity, CloseBillActivityInput{
		BillID:      bill.ID,
		TotalAmount: bill.TotalAmount,
		ClosedAt:    now,
		CloseReason: reason,
	}).Get(ctx, nil)
	if err != nil {
		return BillResult{}, err
	}

	return BillResult{
		BillID:      bill.ID,
		TotalAmount: bill.TotalAmount,
		Currency:    bill.Currency,
		ItemCount:   bill.ItemCount,
		CloseReason: reason,
	}, nil
}

func initialBill(ctx workflow.Context, input BillWorkflowInput) Bill {
	if input.Snapshot != nil {
		return *input.Snapshot
	}
	return Bill{
		ID:          input.BillID,
		AccountID:   input.AccountID,
		Status:      BillStatusOpen,
		Currency:    input.Currency,
		CreatedAt:   workflow.Now(ctx),
		PeriodStart: input.PeriodStart,
		PeriodEnd:   input.PeriodEnd,
	}
}

func initialSeen(input BillWorkflowInput) map[string]struct{} {
	seen := make(map[string]struct{}, len(input.SeenItemIDs))
	for _, id := range input.SeenItemIDs {
		seen[id] = struct{}{}
	}
	return seen
}
