package bill

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

type mockQueryResult struct {
	mock.Mock
}

func (m *mockQueryResult) Get(valuePtr interface{}) error {
	args := m.Called(valuePtr)
	if bill, ok := valuePtr.(*Bill); ok && args.Error(0) == nil {
		*bill = Bill{
			ID:          "test-id",
			Status:      BillStatusOpen,
			Currency:    CurrencyUSD,
			LineItems:   []LineItem{},
			TotalAmount: 0,
		}
	}
	return args.Error(0)
}

func (m *mockQueryResult) HasValue() bool {
	return true
}

func newTestService(t *testing.T) (*Service, *mocks.Client) {
	t.Helper()
	mockClient := &mocks.Client{}
	return &Service{temporalClient: mockClient}, mockClient
}

func TestCreateBill_InvalidCurrency(t *testing.T) {
	svc, _ := newTestService(t)

	_, err := svc.CreateBill(context.Background(), &CreateBillRequest{
		Currency: "INVALID",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid currency")
}

func TestCreateBill_Success(t *testing.T) {
	svc, mockClient := newTestService(t)

	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("bill-some-uuid")
	mockRun.On("GetRunID").Return("run-1")

	mockClient.On("ExecuteWorkflow",
		mock.Anything,
		mock.AnythingOfType("internal.StartWorkflowOptions"),
		mock.AnythingOfType("func(internal.Context, bill.BillWorkflowInput) (bill.BillResult, error)"),
		mock.AnythingOfType("bill.BillWorkflowInput"),
	).Return(mockRun, nil)

	resp, err := svc.CreateBill(context.Background(), &CreateBillRequest{
		Currency: "USD",
	})

	require.NoError(t, err)
	assert.Equal(t, "USD", resp.Currency)
	assert.Equal(t, "OPEN", resp.Status)
	assert.NotEmpty(t, resp.BillID)
}

func TestAddLineItem_InvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		req     *AddLineItemRequest
		wantErr string
	}{
		{
			name:    "empty description",
			req:     &AddLineItemRequest{Description: "", AmountMinor: 1000, Currency: "USD"},
			wantErr: "description is required",
		},
		{
			name:    "zero amount",
			req:     &AddLineItemRequest{Description: "Fee", AmountMinor: 0, Currency: "USD"},
			wantErr: "amountMinor must be positive",
		},
		{
			name:    "negative amount",
			req:     &AddLineItemRequest{Description: "Fee", AmountMinor: -100, Currency: "USD"},
			wantErr: "amountMinor must be positive",
		},
		{
			name:    "invalid currency",
			req:     &AddLineItemRequest{Description: "Fee", AmountMinor: 1000, Currency: "EUR"},
			wantErr: "invalid currency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _ := newTestService(t)
			_, err := svc.AddLineItem(context.Background(), "bill-1", tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestAddLineItem_ClosedBill(t *testing.T) {
	svc, mockClient := newTestService(t)

	mockClient.On("SignalWorkflow",
		mock.Anything,
		"bill-closed-bill",
		"",
		SignalAddLineItem,
		mock.Anything,
	).Return(assert.AnError)

	_, err := svc.AddLineItem(context.Background(), "closed-bill", &AddLineItemRequest{
		Description: "Fee",
		AmountMinor: 1000,
		Currency:    "USD",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot add item")
}

func TestCloseBill_WorkflowNotFound(t *testing.T) {
	svc, mockClient := newTestService(t)

	mockClient.On("SignalWorkflow",
		mock.Anything,
		"bill-nonexistent",
		"",
		SignalCloseBill,
		mock.Anything,
	).Return(assert.AnError)

	_, err := svc.CloseBill(context.Background(), "nonexistent")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot close bill")
}

func TestGetBill_FromWorkflow(t *testing.T) {
	svc, mockClient := newTestService(t)

	qr := &mockQueryResult{}
	qr.On("Get", mock.AnythingOfType("*bill.Bill")).Return(nil)

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-existing",
		"",
		QueryBillState,
	).Return(qr, nil)

	resp, err := svc.GetBill(context.Background(), "existing")

	require.NoError(t, err)
	assert.Equal(t, "test-id", resp.Bill.ID)
	assert.Equal(t, BillStatusOpen, resp.Bill.Status)
}

func TestGetBill_NotFound(t *testing.T) {
	svc, mockClient := newTestService(t)

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-nonexistent",
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), assert.AnError)

	_, err := svc.GetBill(context.Background(), "nonexistent")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestMoney_DisplayAmount(t *testing.T) {
	tests := []struct {
		name string
		m    Money
		want string
	}{
		{name: "USD whole", m: Money{Amount: 1000, Currency: CurrencyUSD}, want: "10.00 USD"},
		{name: "USD cents", m: Money{Amount: 1050, Currency: CurrencyUSD}, want: "10.50 USD"},
		{name: "GEL", m: Money{Amount: 2575, Currency: CurrencyGEL}, want: "25.75 GEL"},
		{name: "zero", m: Money{Amount: 0, Currency: CurrencyUSD}, want: "0.00 USD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.m.DisplayAmount())
		})
	}
}

func TestCurrency_Valid(t *testing.T) {
	assert.True(t, CurrencyUSD.Valid())
	assert.True(t, CurrencyGEL.Valid())
	assert.False(t, Currency("EUR").Valid())
	assert.False(t, Currency("").Valid())
}

// Verify that mocks.Client satisfies the client.Client interface at compile time.
var _ client.Client = (*mocks.Client)(nil)
