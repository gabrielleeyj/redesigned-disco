package bill

import (
	"context"
	"testing"
	"time"

	"encore.dev/beta/errs"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
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
			TotalAmount: decimal.Zero,
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
			req:     &AddLineItemRequest{Description: "", Amount: decimal.NewFromInt(10), Currency: "USD"},
			wantErr: "description is required",
		},
		{
			name:    "zero amount",
			req:     &AddLineItemRequest{Description: "Fee", Amount: decimal.Zero, Currency: "USD"},
			wantErr: "amount must be positive",
		},
		{
			name:    "negative amount",
			req:     &AddLineItemRequest{Description: "Fee", Amount: decimal.NewFromInt(-1), Currency: "USD"},
			wantErr: "amount must be positive",
		},
		{
			name:    "unsupported currency",
			req:     &AddLineItemRequest{Description: "Fee", Amount: decimal.NewFromInt(10), Currency: "XYZ"},
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

	mockClient.On("UpdateWorkflow",
		mock.Anything,
		mock.AnythingOfType("internal.UpdateWorkflowOptions"),
	).Return((*mocks.WorkflowUpdateHandle)(nil), &serviceerror.NotFound{Message: "workflow not found"})

	_, err := svc.AddLineItem(context.Background(), "closed-bill", &AddLineItemRequest{
		Description: "Fee",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found or already closed")
}

func TestAddLineItem_TemporalUnavailable(t *testing.T) {
	svc, mockClient := newTestService(t)

	mockClient.On("UpdateWorkflow",
		mock.Anything,
		mock.AnythingOfType("internal.UpdateWorkflowOptions"),
	).Return((*mocks.WorkflowUpdateHandle)(nil), assert.AnError)

	_, err := svc.AddLineItem(context.Background(), "some-bill", &AddLineItemRequest{
		Description: "Fee",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "service unavailable")
}

func TestAddLineItem_ContextCanceled(t *testing.T) {
	svc, mockClient := newTestService(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockClient.On("UpdateWorkflow",
		mock.Anything,
		mock.AnythingOfType("internal.UpdateWorkflowOptions"),
	).Return((*mocks.WorkflowUpdateHandle)(nil), context.Canceled)

	_, err := svc.AddLineItem(ctx, "some-bill", &AddLineItemRequest{
		Description: "Fee",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "canceled")
}

func TestCloseBill_WorkflowNotFound_NoDBRow(t *testing.T) {
	// True 404: workflow is gone AND no persisted bill row exists.
	// closeBillFromDB → sqldb.ErrNoRows → NotFound. Use a well-formed
	// UUID so we exercise the no-rows path, not the column-type reject.
	svc, mockClient := newTestService(t)
	missingID := uuid.NewString()

	mockClient.On("SignalWorkflow",
		mock.Anything,
		"bill-"+missingID,
		"",
		SignalCloseBill,
		mock.Anything,
	).Return(&serviceerror.NotFound{Message: "workflow not found"})

	_, err := svc.CloseBill(context.Background(), missingID)

	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.NotFound, e.Code)
}

func TestCloseBill_IdempotentRetryReturnsPersistedBill(t *testing.T) {
	// Workflow already completed (e.g. previous successful close, or
	// period-end timer fired). SignalWorkflow → NotFound, but the bill
	// row is in the DB. Endpoint must return 200 with persisted state,
	// not a 404.
	svc, mockClient := newTestService(t)

	billID := uuid.NewString()
	closedAt := time.Now().UTC().Truncate(time.Microsecond)
	createdAt := closedAt.Add(-time.Hour)

	_, err := db.Exec(context.Background(), `
		INSERT INTO bills (id, status, currency, total_amount, created_at, closed_at, close_reason)
		VALUES ($1, 'CLOSED', 'USD', $2, $3, $4, 'SIGNAL')`,
		billID, decimal.RequireFromString("42.50"), createdAt, closedAt,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, billID)
	})

	mockClient.On("SignalWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		SignalCloseBill,
		mock.Anything,
	).Return(&serviceerror.NotFound{Message: "workflow not found"})

	resp, err := svc.CloseBill(context.Background(), billID)
	require.NoError(t, err)
	assert.Equal(t, billID, resp.BillID)
	assert.Equal(t, "CLOSED", resp.Status)
	assert.True(t, decimal.RequireFromString("42.50").Equal(resp.TotalAmount))
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

func TestGetBill_QueryUnavailable(t *testing.T) {
	// A non-NotFound query error (e.g. Temporal briefly unavailable or
	// context canceled) must NOT degrade to a DB lookup, otherwise an open
	// bill that lives only in workflow memory gets reported as 404.
	svc, mockClient := newTestService(t)

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-flaky",
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), assert.AnError)

	_, err := svc.GetBill(context.Background(), "flaky")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query bill")
	assert.NotContains(t, err.Error(), "not found")
}

func TestMoney_DisplayAmount(t *testing.T) {
	tests := []struct {
		name string
		m    Money
		want string
	}{
		{name: "USD whole", m: Money{Amount: decimal.NewFromInt(10), Currency: "USD"}, want: "10.00 USD"},
		{name: "USD cents", m: Money{Amount: decimal.RequireFromString("10.50"), Currency: "USD"}, want: "10.50 USD"},
		{name: "GEL", m: Money{Amount: decimal.RequireFromString("25.75"), Currency: "GEL"}, want: "25.75 GEL"},
		{name: "zero USD", m: Money{Amount: decimal.Zero, Currency: "USD"}, want: "0.00 USD"},
		{name: "JPY rounds to whole", m: Money{Amount: decimal.RequireFromString("1234.56"), Currency: "JPY"}, want: "1235 JPY"},
		{name: "BHD three decimals", m: Money{Amount: decimal.RequireFromString("12.345"), Currency: "BHD"}, want: "12.345 BHD"},
		{name: "negative below unit", m: Money{Amount: decimal.RequireFromString("-0.50"), Currency: "USD"}, want: "-0.50 USD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.m.DisplayAmount())
		})
	}
}

func TestCurrency_Valid(t *testing.T) {
	for _, code := range []Currency{"USD", "EUR", "GBP", "GEL", "JPY", "KRW", "BHD", "KWD"} {
		assert.Truef(t, code.Valid(), "%s should be valid", code)
	}
	for _, code := range []Currency{"XYZ", "", "us"} {
		assert.Falsef(t, code.Valid(), "%s should be invalid", code)
	}
}

func TestCurrency_Decimals(t *testing.T) {
	assert.Equal(t, int32(2), Currency("USD").Decimals())
	assert.Equal(t, int32(0), Currency("JPY").Decimals())
	assert.Equal(t, int32(3), Currency("BHD").Decimals())
}

// Verify that mocks.Client satisfies the client.Client interface at compile time.
var _ client.Client = (*mocks.Client)(nil)
