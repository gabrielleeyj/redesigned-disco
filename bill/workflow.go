package bill

import (
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

	err := workflow.SetQueryHandler(ctx, QueryBillState, func() (Bill, error) {
		return bill, nil
	})
	if err != nil {
		return BillResult{}, err
	}

	addItemCh := workflow.GetSignalChannel(ctx, SignalAddLineItem)
	closeCh := workflow.GetSignalChannel(ctx, SignalCloseBill)

	closed := false
	for !closed {
		selector := workflow.NewSelector(ctx)

		selector.AddReceive(addItemCh, func(c workflow.ReceiveChannel, more bool) {
			var signal AddLineItemSignal
			c.Receive(ctx, &signal)
			handleAddLineItem(&bill, seen, signal, workflow.Now(ctx))
		})

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

	drainSignals(ctx, addItemCh, &bill, seen)

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
	err = workflow.ExecuteActivity(activityCtx, PersistBillActivity, bill).Get(ctx, nil)
	if err != nil {
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

func handleAddLineItem(bill *Bill, seen map[string]struct{}, signal AddLineItemSignal, now time.Time) {
	if signal.Currency != bill.Currency {
		return
	}
	if _, dup := seen[signal.ItemID]; dup {
		return
	}

	item := LineItem{
		ID:          signal.ItemID,
		Description: signal.Description,
		Amount: Money{
			Amount:   signal.AmountMinor,
			Currency: signal.Currency,
		},
		CreatedAt: now,
	}

	bill.LineItems = append(bill.LineItems, item)
	bill.TotalAmount += signal.AmountMinor
	seen[signal.ItemID] = struct{}{}
}

func drainSignals(ctx workflow.Context, ch workflow.ReceiveChannel, bill *Bill, seen map[string]struct{}) {
	for {
		var signal AddLineItemSignal
		ok := ch.ReceiveAsync(&signal)
		if !ok {
			break
		}
		handleAddLineItem(bill, seen, signal, workflow.Now(ctx))
	}
}
