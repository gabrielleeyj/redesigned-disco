package bill

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const continueAsNewThreshold = 1000

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
	for !closed {
		selector := workflow.NewSelector(ctx)

		selector.AddReceive(closeCh, func(c workflow.ReceiveChannel, more bool) {
			var signal CloseBillSignal
			c.Receive(ctx, &signal)
			closed = true
		})

		selector.Select(ctx)

		if len(bill.LineItems) >= continueAsNewThreshold && !closed {
			snapshot := bill
			return BillResult{}, workflow.NewContinueAsNewError(ctx, BillingWorkflow, BillWorkflowInput{
				BillID:   input.BillID,
				Currency: input.Currency,
				Snapshot: &snapshot,
			})
		}
	}

	if err := workflow.Await(ctx, func() bool {
		return workflow.AllHandlersFinished(ctx)
	}); err != nil {
		return BillResult{}, fmt.Errorf("await pending updates: %w", err)
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
