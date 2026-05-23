package bill

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestBillingWorkflow(t *testing.T) {
	tests := []struct {
		name      string
		input     BillWorkflowInput
		signals   []any
		wantTotal int64
		wantItems int
	}{
		{
			name:  "create and close empty bill",
			input: BillWorkflowInput{BillID: "bill-1", Currency: CurrencyUSD},
			signals: []any{
				CloseBillSignal{},
			},
			wantTotal: 0,
			wantItems: 0,
		},
		{
			name:  "add three items then close",
			input: BillWorkflowInput{BillID: "bill-2", Currency: CurrencyUSD},
			signals: []any{
				AddLineItemSignal{ItemID: "i1", Description: "Fee A", AmountMinor: 1000, Currency: CurrencyUSD},
				AddLineItemSignal{ItemID: "i2", Description: "Fee B", AmountMinor: 2500, Currency: CurrencyUSD},
				AddLineItemSignal{ItemID: "i3", Description: "Fee C", AmountMinor: 500, Currency: CurrencyUSD},
				CloseBillSignal{},
			},
			wantTotal: 4000,
			wantItems: 3,
		},
		{
			name:  "wrong currency item rejected",
			input: BillWorkflowInput{BillID: "bill-3", Currency: CurrencyUSD},
			signals: []any{
				AddLineItemSignal{ItemID: "i1", Description: "Valid", AmountMinor: 1000, Currency: CurrencyUSD},
				AddLineItemSignal{ItemID: "i2", Description: "Invalid GEL", AmountMinor: 500, Currency: CurrencyGEL},
				CloseBillSignal{},
			},
			wantTotal: 1000,
			wantItems: 1,
		},
		{
			name:  "idempotent - duplicate item ID ignored",
			input: BillWorkflowInput{BillID: "bill-4", Currency: CurrencyUSD},
			signals: []any{
				AddLineItemSignal{ItemID: "dup", Description: "First", AmountMinor: 1000, Currency: CurrencyUSD},
				AddLineItemSignal{ItemID: "dup", Description: "Duplicate", AmountMinor: 1000, Currency: CurrencyUSD},
				CloseBillSignal{},
			},
			wantTotal: 1000,
			wantItems: 1,
		},
		{
			name:  "GEL bill works independently",
			input: BillWorkflowInput{BillID: "bill-5", Currency: CurrencyGEL},
			signals: []any{
				AddLineItemSignal{ItemID: "g1", Description: "GEL Fee", AmountMinor: 3000, Currency: CurrencyGEL},
				AddLineItemSignal{ItemID: "g2", Description: "GEL Fee 2", AmountMinor: 1500, Currency: CurrencyGEL},
				CloseBillSignal{},
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

			for _, sig := range tt.signals {
				switch s := sig.(type) {
				case AddLineItemSignal:
					env.RegisterDelayedCallback(func() {
						env.SignalWorkflow(SignalAddLineItem, s)
					}, 0)
				case CloseBillSignal:
					env.RegisterDelayedCallback(func() {
						env.SignalWorkflow(SignalCloseBill, s)
					}, 0)
				}
			}

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

func TestBillingWorkflow_QueryState(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivity(PersistBillActivity)
	env.OnActivity(PersistBillActivity, mock.Anything, mock.Anything).Return(nil)

	input := BillWorkflowInput{BillID: "q-bill", Currency: CurrencyUSD}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalAddLineItem, AddLineItemSignal{
			ItemID:      "q1",
			Description: "Query test",
			AmountMinor: 2000,
			Currency:    CurrencyUSD,
		})
	}, 0)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseBillSignal{})
	}, 0)

	env.ExecuteWorkflow(BillingWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "q-bill", result.BillID)
	assert.Equal(t, int64(2000), result.TotalAmount)
	assert.Equal(t, 1, result.ItemCount)
}
