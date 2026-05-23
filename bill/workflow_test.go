package bill

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

type addStep struct {
	in AddLineItemInput
	// wantRejected is true when the validator should reject this update.
	wantRejected bool
}

func TestBillingWorkflow(t *testing.T) {
	tests := []struct {
		name      string
		input     BillWorkflowInput
		adds      []addStep
		wantTotal int64
		wantItems int
	}{
		{
			name:      "create and close empty bill",
			input:     BillWorkflowInput{BillID: "bill-1", Currency: CurrencyUSD},
			adds:      nil,
			wantTotal: 0,
			wantItems: 0,
		},
		{
			name:  "add three items then close",
			input: BillWorkflowInput{BillID: "bill-2", Currency: CurrencyUSD},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "i1", Description: "Fee A", AmountMinor: 1000, Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "i2", Description: "Fee B", AmountMinor: 2500, Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "i3", Description: "Fee C", AmountMinor: 500, Currency: CurrencyUSD}},
			},
			wantTotal: 4000,
			wantItems: 3,
		},
		{
			name:  "wrong currency item rejected by validator",
			input: BillWorkflowInput{BillID: "bill-3", Currency: CurrencyUSD},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "i1", Description: "Valid", AmountMinor: 1000, Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "i2", Description: "Invalid GEL", AmountMinor: 500, Currency: CurrencyGEL}, wantRejected: true},
			},
			wantTotal: 1000,
			wantItems: 1,
		},
		{
			name:  "idempotent - duplicate item ID ignored",
			input: BillWorkflowInput{BillID: "bill-4", Currency: CurrencyUSD},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "dup", Description: "First", AmountMinor: 1000, Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "dup", Description: "Duplicate", AmountMinor: 1000, Currency: CurrencyUSD}},
			},
			wantTotal: 1000,
			wantItems: 1,
		},
		{
			name:  "GEL bill works independently",
			input: BillWorkflowInput{BillID: "bill-5", Currency: CurrencyGEL},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "g1", Description: "GEL Fee", AmountMinor: 3000, Currency: CurrencyGEL}},
				{in: AddLineItemInput{ItemID: "g2", Description: "GEL Fee 2", AmountMinor: 1500, Currency: CurrencyGEL}},
			},
			wantTotal: 4500,
			wantItems: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()

			env.RegisterActivity(PersistBillActivity)
			env.OnActivity(PersistBillActivity, mock.Anything, mock.Anything).Return(nil)

			for i, step := range tt.adds {
				step := step
				env.RegisterDelayedCallback(func() {
					env.UpdateWorkflow(UpdateAddLineItem, "", &testsuite.TestUpdateCallback{
						OnAccept:   func() {},
						OnReject:   func(err error) { assert.True(t, step.wantRejected, "step %d rejected unexpectedly: %v", i, err) },
						OnComplete: func(_ interface{}, err error) { assert.NoError(t, err, "step %d", i) },
					}, step.in)
				}, time.Duration(i+1)*time.Millisecond)
			}

			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(SignalCloseBill, CloseBillSignal{})
			}, time.Duration(len(tt.adds)+1)*time.Millisecond)

			env.ExecuteWorkflow(BillingWorkflow, tt.input)

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var result BillResult
			require.NoError(t, env.GetWorkflowResult(&result))

			assert.Equal(t, tt.input.BillID, result.BillID)
			assert.Equal(t, tt.wantTotal, result.TotalAmount)
			assert.Equal(t, tt.wantItems, result.ItemCount)
			assert.Equal(t, tt.input.Currency, result.Currency)
		})
	}
}

func TestBillingWorkflow_ActivityRetried(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivity(PersistBillActivity)

	attempts := 0
	env.OnActivity(PersistBillActivity, mock.Anything, mock.Anything).Return(func(_ interface{}, _ Bill) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient db failure")
		}
		return nil
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseBillSignal{})
	}, time.Millisecond)

	env.ExecuteWorkflow(BillingWorkflow, BillWorkflowInput{BillID: "retry-bill", Currency: CurrencyUSD})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.GreaterOrEqual(t, attempts, 3, "activity should retry until success")
}
