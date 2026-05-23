package bill

import (
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
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

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestBillingWorkflow(t *testing.T) {
	tests := []struct {
		name      string
		input     BillWorkflowInput
		adds      []addStep
		wantTotal decimal.Decimal
		wantItems int
	}{
		{
			name:      "create and close empty bill",
			input:     BillWorkflowInput{BillID: "bill-1", Currency: CurrencyUSD},
			adds:      nil,
			wantTotal: decimal.Zero,
			wantItems: 0,
		},
		{
			name:  "add three items then close",
			input: BillWorkflowInput{BillID: "bill-2", Currency: CurrencyUSD},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "i1", Description: "Fee A", Amount: dec("10.00"), Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "i2", Description: "Fee B", Amount: dec("25.00"), Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "i3", Description: "Fee C", Amount: dec("5.00"), Currency: CurrencyUSD}},
			},
			wantTotal: dec("40.00"),
			wantItems: 3,
		},
		{
			name:  "sub-cent precision preserved across additions",
			input: BillWorkflowInput{BillID: "bill-precise", Currency: "USD"},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "p1", Description: "Micro fee", Amount: dec("0.0001"), Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "p2", Description: "Interest", Amount: dec("0.123456789"), Currency: CurrencyUSD}},
			},
			wantTotal: dec("0.123556789"),
			wantItems: 2,
		},
		{
			name:  "wrong currency item rejected by validator",
			input: BillWorkflowInput{BillID: "bill-3", Currency: CurrencyUSD},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "i1", Description: "Valid", Amount: dec("10.00"), Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "i2", Description: "Invalid GEL", Amount: dec("5.00"), Currency: CurrencyGEL}, wantRejected: true},
			},
			wantTotal: dec("10.00"),
			wantItems: 1,
		},
		{
			name:  "idempotent - duplicate item ID ignored",
			input: BillWorkflowInput{BillID: "bill-4", Currency: CurrencyUSD},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "dup", Description: "First", Amount: dec("10.00"), Currency: CurrencyUSD}},
				{in: AddLineItemInput{ItemID: "dup", Description: "Duplicate", Amount: dec("10.00"), Currency: CurrencyUSD}},
			},
			wantTotal: dec("10.00"),
			wantItems: 1,
		},
		{
			name:  "JPY bill (zero decimals) works",
			input: BillWorkflowInput{BillID: "bill-jpy", Currency: "JPY"},
			adds: []addStep{
				{in: AddLineItemInput{ItemID: "j1", Description: "Yen fee", Amount: dec("1000"), Currency: "JPY"}},
				{in: AddLineItemInput{ItemID: "j2", Description: "Yen fee 2", Amount: dec("500"), Currency: "JPY"}},
			},
			wantTotal: dec("1500"),
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
			assert.True(t, tt.wantTotal.Equal(result.TotalAmount), "want %s got %s", tt.wantTotal, result.TotalAmount)
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
