package bill

import (
	"context"
	"fmt"
	"testing"
	"time"

	"encore.dev/beta/auth"
	"encore.dev/beta/errs"
	"encore.dev/et"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
)

const testAccountID = "acct-test"

// withTestAuth sets the per-test auth identity so endpoint methods
// that call callerAccountID see a non-empty account. Without this,
// auth.Data() returns nil and ownership checks fail.
func withTestAuth(t *testing.T) {
	t.Helper()
	et.OverrideAuthInfo(auth.UID(testAccountID), &AuthData{AccountID: testAccountID})
}

type mockQueryResult struct {
	mock.Mock
	fill *Bill
}

func (m *mockQueryResult) Get(valuePtr interface{}) error {
	args := m.Called(valuePtr)
	if bill, ok := valuePtr.(*Bill); ok && args.Error(0) == nil {
		if m.fill != nil {
			*bill = *m.fill
		} else {
			*bill = Bill{
				ID:          "test-id",
				AccountID:   testAccountID,
				Status:      BillStatusOpen,
				Currency:    CurrencyUSD,
				LineItems:   []LineItem{},
				TotalAmount: decimal.Zero,
			}
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
	withTestAuth(t)
	svc, _ := newTestService(t)

	_, err := svc.CreateBill(context.Background(), &CreateBillRequest{
		Currency: "INVALID",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid currency")
}

func TestCreateBill_Success(t *testing.T) {
	withTestAuth(t)
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

func TestNewBillID_DeterministicForSameAccountAndKey(t *testing.T) {
	a := newBillID("acct-1", "key-A")
	b := newBillID("acct-1", "key-A")
	assert.Equal(t, a, b)
}

func TestNewBillID_DifferentForDifferentAccounts(t *testing.T) {
	a := newBillID("acct-1", "key-A")
	b := newBillID("acct-2", "key-A")
	assert.NotEqual(t, a, b, "same key under different accounts must produce different bill IDs")
}

func TestNewBillID_RandomWhenNoKey(t *testing.T) {
	a := newBillID("acct-1", "")
	b := newBillID("acct-1", "")
	assert.NotEqual(t, a, b, "missing key should yield fresh UUIDs each call")
}

func TestCreateBill_IdempotentRetryReturnsSameBillID(t *testing.T) {
	// Same (account, idempotency key) → same workflow ID. Temporal
	// returns WorkflowExecutionAlreadyStarted on the retry; endpoint
	// surfaces the existing bill instead of failing with AlreadyExists.
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("bill-some-uuid")
	mockRun.On("GetRunID").Return("run-1")

	// First attempt succeeds.
	mockClient.On("ExecuteWorkflow",
		mock.Anything,
		mock.AnythingOfType("internal.StartWorkflowOptions"),
		mock.AnythingOfType("func(internal.Context, bill.BillWorkflowInput) (bill.BillResult, error)"),
		mock.AnythingOfType("bill.BillWorkflowInput"),
	).Return(mockRun, nil).Once()

	// Second attempt with the same key → already-started.
	mockClient.On("ExecuteWorkflow",
		mock.Anything,
		mock.AnythingOfType("internal.StartWorkflowOptions"),
		mock.AnythingOfType("func(internal.Context, bill.BillWorkflowInput) (bill.BillResult, error)"),
		mock.AnythingOfType("bill.BillWorkflowInput"),
	).Return((*mocks.WorkflowRun)(nil), &serviceerror.WorkflowExecutionAlreadyStarted{Message: "exists"}).Once()

	// On retry, loadBill is consulted to surface authoritative state.
	// Return a workflow query result with matching account.
	expectedID := newBillID(testAccountID, "create-key-1")
	qr := &mockQueryResult{
		fill: &Bill{
			ID:        expectedID,
			AccountID: testAccountID,
			Status:    BillStatusOpen,
			Currency:  CurrencyUSD,
		},
	}
	qr.On("Get", mock.AnythingOfType("*bill.Bill")).Return(nil)
	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+expectedID,
		"",
		QueryBillState,
	).Return(qr, nil)

	req := &CreateBillRequest{Currency: "USD", IdempotencyKey: "create-key-1"}

	first, err := svc.CreateBill(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, expectedID, first.BillID)

	second, err := svc.CreateBill(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, first.BillID, second.BillID, "idempotent retry must return the same bill")
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
			withTestAuth(t)
			svc, _ := newTestService(t)
			_, err := svc.AddLineItem(context.Background(), "bill-1", tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestAddLineItem_ClosedBill(t *testing.T) {
	withTestAuth(t)
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

func TestClassifyUpdateError_CurrencyMismatchIsFailedPrecondition(t *testing.T) {
	// The workflow validator returns ApplicationError with type
	// CurrencyMismatch when an item's currency does not match the bill's
	// locked-in currency. That is a state conflict, not a bad request —
	// the endpoint must surface it as FailedPrecondition (409) so
	// callers can distinguish "fix the payload" from "fix the routing".
	mismatchErr := temporal.NewApplicationError(
		"currency mismatch: bill is USD, item is GEL",
		currencyMismatchErrType,
	)
	e := classifyUpdateError(mismatchErr)
	require.NotNil(t, e)
	assert.Equal(t, errs.FailedPrecondition, e.Code)
	assert.Contains(t, e.Message, "currency mismatch")
}

func TestClassifyUpdateError_OtherAppErrorIsInvalidArgument(t *testing.T) {
	other := temporal.NewApplicationError("amount must be positive", "ValidationError")
	e := classifyUpdateError(other)
	require.NotNil(t, e)
	assert.Equal(t, errs.InvalidArgument, e.Code)
}

func TestAddLineItem_TemporalUnavailable(t *testing.T) {
	withTestAuth(t)
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
	withTestAuth(t)
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

func TestAddLineItem_BillNotFoundFromOwnershipMismatch(t *testing.T) {
	// The workflow validator rejects updates whose CallerAccountID does
	// not match the bill's AccountID. The endpoint must surface that as
	// a 404 — not a 403 — so non-owners cannot probe for the existence
	// of other tenants' bill IDs.
	notFoundErr := temporal.NewApplicationError("bill not found", billNotFoundErrType)
	e := classifyUpdateError(notFoundErr)
	require.NotNil(t, e)
	assert.Equal(t, errs.NotFound, e.Code)
}

func TestCloseBill_WorkflowNotFound_NoDBRow(t *testing.T) {
	// True 404: workflow is gone AND no persisted bill row exists.
	// assertOwnsBill → loadBill → QueryWorkflow returns NotFound → DB
	// returns ErrNoRows → endpoint surfaces NotFound. SignalWorkflow
	// is never reached because the ownership pre-check short-circuits.
	withTestAuth(t)
	svc, mockClient := newTestService(t)
	missingID := uuid.NewString()

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+missingID,
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), &serviceerror.NotFound{Message: "workflow not found"})

	_, err := svc.CloseBill(context.Background(), missingID)

	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.NotFound, e.Code)
}

func TestCloseBill_IdempotentRetryReturnsPersistedBill(t *testing.T) {
	// Workflow already completed (e.g. previous successful close, or
	// period-end timer fired). Ownership pre-check loads from DB
	// (Temporal NotFound → fallback), confirms account match, then
	// SignalWorkflow also returns NotFound — endpoint returns 200 with
	// persisted state.
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	billID := uuid.NewString()
	closedAt := time.Now().UTC().Truncate(time.Microsecond)
	createdAt := closedAt.Add(-time.Hour)

	_, err := db.Exec(context.Background(), `
		INSERT INTO bills (id, account_id, status, currency, total_amount, created_at, closed_at, close_reason)
		VALUES ($1, $2, 'CLOSED', 'USD', $3, $4, $5, 'SIGNAL')`,
		billID, testAccountID, decimal.RequireFromString("42.50"), createdAt, closedAt,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, billID)
	})

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), &serviceerror.NotFound{Message: "workflow not found"})

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

func TestCloseBill_OwnershipMismatchReturns404(t *testing.T) {
	// Bill belongs to another account. assertOwnsBill returns NotFound
	// (not PermissionDenied) so existence is not leaked.
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	billID := uuid.NewString()
	_, err := db.Exec(context.Background(), `
		INSERT INTO bills (id, account_id, status, currency, total_amount, created_at)
		VALUES ($1, 'other-account', 'OPEN', 'USD', 0, NOW())`,
		billID,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, billID)
	})

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), &serviceerror.NotFound{Message: "workflow not found"})

	_, err = svc.CloseBill(context.Background(), billID)
	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.NotFound, e.Code)
}

func TestGetBill_FromWorkflow(t *testing.T) {
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	billID := uuid.NewString()
	qr := &mockQueryResult{
		fill: &Bill{
			ID:        billID,
			AccountID: testAccountID,
			Status:    BillStatusOpen,
			Currency:  CurrencyUSD,
		},
	}
	qr.On("Get", mock.AnythingOfType("*bill.Bill")).Return(nil)

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		QueryBillState,
	).Return(qr, nil)

	resp, err := svc.GetBill(context.Background(), billID)

	require.NoError(t, err)
	assert.Equal(t, billID, resp.Bill.ID)
	assert.Equal(t, BillStatusOpen, resp.Bill.Status)
	assert.Empty(t, resp.Bill.LineItems, "no items inserted, fetch should return empty")
}

func TestGetBill_QueryUnavailable(t *testing.T) {
	// A non-NotFound query error (e.g. Temporal briefly unavailable or
	// context canceled) must NOT degrade to a DB lookup, otherwise an open
	// bill that lives only in workflow memory gets reported as 404.
	withTestAuth(t)
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

func TestGetBill_OwnershipMismatchReturns404(t *testing.T) {
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	billID := uuid.NewString()
	qr := &mockQueryResult{
		fill: &Bill{
			ID:        billID,
			AccountID: "other-account",
			Status:    BillStatusOpen,
			Currency:  CurrencyUSD,
		},
	}
	qr.On("Get", mock.AnythingOfType("*bill.Bill")).Return(nil)

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		QueryBillState,
	).Return(qr, nil)

	_, err := svc.GetBill(context.Background(), billID)
	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.NotFound, e.Code)
}

func TestListLineItems_PagesThroughCursor(t *testing.T) {
	// Insert N items, page through with limit=2, verify ordering and
	// that the cursor consumes the whole set with no duplicates or
	// gaps. Uses the real DB so the keyset predicate is exercised.
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	billID := uuid.NewString()
	_, err := db.Exec(context.Background(), `
		INSERT INTO bills (id, account_id, status, currency, total_amount, created_at)
		VALUES ($1, $2, 'OPEN', 'USD', 0, NOW())`,
		billID, testAccountID,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM line_items WHERE bill_id = $1`, billID)
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, billID)
	})

	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 5; i++ {
		_, err := db.Exec(context.Background(), `
			INSERT INTO line_items (id, bill_id, description, amount, currency, created_at)
			VALUES ($1, $2, $3, 1, 'USD', $4)`,
			uuid.NewString(), billID, fmt.Sprintf("item-%d", i), base.Add(time.Duration(i)*time.Second),
		)
		require.NoError(t, err)
	}

	// Stub the ownership pre-check: workflow query says NotFound so
	// the path falls through to the DB-backed bill row (real).
	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), &serviceerror.NotFound{Message: "completed"})

	collected := []string{}
	cursor := ""
	for {
		resp, err := svc.ListLineItems(context.Background(), billID, &ListLineItemsRequest{
			Cursor: cursor,
			Limit:  2,
		})
		require.NoError(t, err)
		for _, item := range resp.Items {
			collected = append(collected, item.Description)
		}
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}

	assert.Equal(t, []string{"item-0", "item-1", "item-2", "item-3", "item-4"}, collected)
}

func TestListLineItems_RejectsMalformedCursor(t *testing.T) {
	withTestAuth(t)
	svc, mockClient := newTestService(t)
	billID := uuid.NewString()

	_, err := db.Exec(context.Background(), `
		INSERT INTO bills (id, account_id, status, currency, total_amount, created_at)
		VALUES ($1, $2, 'OPEN', 'USD', 0, NOW())`,
		billID, testAccountID,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, billID)
	})

	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+billID,
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), &serviceerror.NotFound{Message: "completed"})

	_, err = svc.ListLineItems(context.Background(), billID, &ListLineItemsRequest{
		Cursor: "not-base64!!!",
	})
	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.InvalidArgument, e.Code)
}

func TestListBills_FilterByStatusAndKeysetPagination(t *testing.T) {
	// Insert 3 OPEN + 2 CLOSED bills under testAccountID and 1 bill
	// under another account (to confirm tenancy scoping). Verify:
	//  - default returns all 5 of the caller's bills DESC by created_at
	//  - status=OPEN returns the 3 OPEN bills
	//  - keyset pagination walks the whole set with no duplicates/gaps
	//  - other-account bill never appears
	withTestAuth(t)
	svc, _ := newTestService(t)

	base := time.Now().UTC().Truncate(time.Microsecond)
	ids := make([]string, 5)
	statuses := []BillStatus{BillStatusOpen, BillStatusClosed, BillStatusOpen, BillStatusClosed, BillStatusOpen}
	for i := range ids {
		ids[i] = uuid.NewString()
		_, err := db.Exec(context.Background(), `
			INSERT INTO bills (id, account_id, status, currency, total_amount, created_at)
			VALUES ($1, $2, $3, 'USD', 0, $4)`,
			ids[i], testAccountID, string(statuses[i]), base.Add(time.Duration(i)*time.Second),
		)
		require.NoError(t, err)
	}
	otherID := uuid.NewString()
	_, err := db.Exec(context.Background(), `
		INSERT INTO bills (id, account_id, status, currency, total_amount, created_at)
		VALUES ($1, 'other-account', 'OPEN', 'USD', 0, $2)`,
		otherID, base,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE account_id IN ($1, 'other-account')`, testAccountID)
	})

	// All five, paginated with limit=2.
	collected := []string{}
	cursor := ""
	for {
		resp, err := svc.ListBills(context.Background(), &ListBillsRequest{Cursor: cursor, Limit: 2})
		require.NoError(t, err)
		for _, b := range resp.Bills {
			collected = append(collected, b.ID)
			assert.NotEqual(t, otherID, b.ID, "other-account bill must never appear")
		}
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	// Order is created_at DESC, so the newest (index 4) first.
	expected := []string{ids[4], ids[3], ids[2], ids[1], ids[0]}
	assert.Equal(t, expected, collected)

	// Status filter — only OPEN.
	openResp, err := svc.ListBills(context.Background(), &ListBillsRequest{Status: "OPEN", Limit: 100})
	require.NoError(t, err)
	openIDs := make([]string, 0, len(openResp.Bills))
	for _, b := range openResp.Bills {
		assert.Equal(t, BillStatusOpen, b.Status)
		openIDs = append(openIDs, b.ID)
	}
	assert.ElementsMatch(t, []string{ids[0], ids[2], ids[4]}, openIDs)
}

func TestListBills_RejectsInvalidStatus(t *testing.T) {
	withTestAuth(t)
	svc, _ := newTestService(t)

	_, err := svc.ListBills(context.Background(), &ListBillsRequest{Status: "PENDING"})
	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.InvalidArgument, e.Code)
}

func TestBillEvents_AppendOnlyAndOrderedByActivities(t *testing.T) {
	// Drive the three activities directly against the real DB to
	// verify each writes its event, the append-only trigger holds,
	// and ListBillEvents returns them in order. Activities are the
	// only producers; the endpoint only reads.
	withTestAuth(t)
	svc, mockClient := newTestService(t)

	bill := Bill{
		ID:        uuid.NewString(),
		AccountID: testAccountID,
		Status:    BillStatusOpen,
		Currency:  CurrencyUSD,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bill_events WHERE bill_id = $1`, bill.ID)
		_, _ = db.Exec(context.Background(), `DELETE FROM line_items WHERE bill_id = $1`, bill.ID)
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, bill.ID)
	})

	require.NoError(t, CreateBillActivity(context.Background(), bill))

	item := LineItem{
		ID: uuid.NewString(), Description: "fee", Amount: decimal.NewFromInt(10),
		Currency: CurrencyUSD, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	require.NoError(t, AppendLineItemActivity(context.Background(), AppendLineItemInput{
		BillID: bill.ID, Actor: testAccountID, Item: item,
	}))

	require.NoError(t, CloseBillActivity(context.Background(), CloseBillActivityInput{
		BillID: bill.ID, Actor: testAccountID, TotalAmount: decimal.NewFromInt(10),
		ClosedAt: time.Now().UTC().Truncate(time.Microsecond), CloseReason: CloseReasonSignal,
	}))

	// Read events back via the endpoint. assertOwnsBill falls back to
	// DB after a Temporal NotFound (workflow never existed).
	mockClient.On("QueryWorkflow",
		mock.Anything,
		"bill-"+bill.ID,
		"",
		QueryBillState,
	).Return((*mockQueryResult)(nil), &serviceerror.NotFound{Message: "no workflow"})

	resp, err := svc.ListBillEvents(context.Background(), bill.ID, &ListBillEventsRequest{Limit: 50})
	require.NoError(t, err)
	require.Len(t, resp.Events, 3, "expected OPENED + ITEM_ADDED + CLOSED")
	assert.Equal(t, BillEventOpened, resp.Events[0].Kind)
	assert.Equal(t, BillEventItemAdded, resp.Events[1].Kind)
	assert.Equal(t, BillEventClosed, resp.Events[2].Kind)
	assert.Equal(t, testAccountID, resp.Events[0].Actor)

	// Append-only trigger: UPDATE and DELETE must be blocked.
	_, err = db.Exec(context.Background(), `UPDATE bill_events SET actor = 'tamper' WHERE bill_id = $1`, bill.ID)
	require.Error(t, err, "UPDATE on bill_events must be blocked by trigger")
	_, err = db.Exec(context.Background(), `DELETE FROM bill_events WHERE bill_id = $1`, bill.ID)
	require.Error(t, err, "DELETE on bill_events must be blocked by trigger")
}

func TestAppendLineItemActivity_DuplicateIsIdempotent(t *testing.T) {
	// Run twice with the same item; assert single line_item row,
	// single ITEM_ADDED event, and bills.total_amount = item.Amount
	// (not 2x).
	bill := Bill{
		ID: uuid.NewString(), AccountID: testAccountID, Status: BillStatusOpen,
		Currency: CurrencyUSD, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM bill_events WHERE bill_id = $1`, bill.ID)
		_, _ = db.Exec(context.Background(), `DELETE FROM line_items WHERE bill_id = $1`, bill.ID)
		_, _ = db.Exec(context.Background(), `DELETE FROM bills WHERE id = $1`, bill.ID)
	})
	require.NoError(t, CreateBillActivity(context.Background(), bill))

	item := LineItem{
		ID: uuid.NewString(), Description: "fee", Amount: decimal.NewFromInt(10),
		Currency: CurrencyUSD, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	in := AppendLineItemInput{BillID: bill.ID, Actor: testAccountID, Item: item}
	require.NoError(t, AppendLineItemActivity(context.Background(), in))
	require.NoError(t, AppendLineItemActivity(context.Background(), in))

	var itemCount, eventCount int
	require.NoError(t, db.QueryRow(context.Background(),
		`SELECT count(*) FROM line_items WHERE bill_id = $1`, bill.ID).Scan(&itemCount))
	require.NoError(t, db.QueryRow(context.Background(),
		`SELECT count(*) FROM bill_events WHERE bill_id = $1 AND kind = 'ITEM_ADDED'`, bill.ID).Scan(&eventCount))

	var total decimal.Decimal
	require.NoError(t, db.QueryRow(context.Background(),
		`SELECT total_amount FROM bills WHERE id = $1`, bill.ID).Scan(&total))

	assert.Equal(t, 1, itemCount, "duplicate item must not insert twice")
	assert.Equal(t, 1, eventCount, "duplicate item must not emit a second event")
	assert.True(t, decimal.NewFromInt(10).Equal(total), "total must not double-count")
}

func TestRefreshCurrencies_RebuildsCacheFromDB(t *testing.T) {
	// Wipe the cache then call the endpoint. The reload pulls from
	// the seeded migration; the response count should match what we
	// inserted in migration 2_currencies.up.sql.
	withTestAuth(t)
	svc, _ := newTestService(t)

	// Override with an empty map to prove the refresh actually
	// repopulates rather than reading a stale value.
	setCurrencies(map[Currency]CurrencyMeta{})

	resp, err := svc.RefreshCurrencies(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.CurrencyCount, 4, "expected at least USD/EUR/GBP/GEL from seed")
	assert.True(t, Currency("USD").Valid(), "registry should know USD again after refresh")
}

func TestAuthHandler_RejectsMissingHeader(t *testing.T) {
	_, _, err := AuthHandler(context.Background(), &AuthParams{AccountID: ""})
	require.Error(t, err)
	var e *errs.Error
	require.ErrorAs(t, err, &e)
	assert.Equal(t, errs.Unauthenticated, e.Code)
}

func TestAuthHandler_AcceptsNonEmptyHeader(t *testing.T) {
	uid, data, err := AuthHandler(context.Background(), &AuthParams{AccountID: "acct-42"})
	require.NoError(t, err)
	assert.Equal(t, auth.UID("acct-42"), uid)
	require.NotNil(t, data)
	assert.Equal(t, "acct-42", data.AccountID)
}

func TestCurrency_FormatAmount(t *testing.T) {
	tests := []struct {
		name     string
		currency Currency
		amount   decimal.Decimal
		want     string
	}{
		{name: "USD whole", currency: "USD", amount: decimal.NewFromInt(10), want: "10.00 USD"},
		{name: "USD cents", currency: "USD", amount: decimal.RequireFromString("10.50"), want: "10.50 USD"},
		{name: "GEL", currency: "GEL", amount: decimal.RequireFromString("25.75"), want: "25.75 GEL"},
		{name: "zero USD", currency: "USD", amount: decimal.Zero, want: "0.00 USD"},
		{name: "JPY rounds to whole", currency: "JPY", amount: decimal.RequireFromString("1234.56"), want: "1235 JPY"},
		{name: "BHD three decimals", currency: "BHD", amount: decimal.RequireFromString("12.345"), want: "12.345 BHD"},
		{name: "negative below unit", currency: "USD", amount: decimal.RequireFromString("-0.50"), want: "-0.50 USD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.currency.FormatAmount(tt.amount))
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
