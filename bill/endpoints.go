package bill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encore.dev/beta/errs"
	"encore.dev/rlog"
	"encore.dev/storage/sqldb"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// emptyRunID targets the latest run of a workflow ID. Required when the
// workflow may have transitioned through ContinueAsNew.
const emptyRunID = ""

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

type CreateBillRequest struct {
	Currency string `json:"currency"`
	// PeriodStart and PeriodEnd bracket the fee accrual window. Both are
	// optional; when PeriodEnd is set, the workflow auto-closes when the
	// timer fires (no explicit CloseBill needed). When unset, the bill
	// stays open until explicitly closed.
	PeriodStart *time.Time `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time `json:"periodEnd,omitempty"`
	// IdempotencyKey makes CreateBill safe to retry. When set, the bill
	// ID is derived deterministically from (account, key) so a network
	// retry returns the same bill instead of creating a duplicate. The
	// key is scoped per-account; the same key value across accounts
	// produces different bills.
	IdempotencyKey string `header:"Idempotency-Key"`
}

type CreateBillResponse struct {
	BillID      string     `json:"billId"`
	Status      string     `json:"status"`
	Currency    string     `json:"currency"`
	PeriodStart *time.Time `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time `json:"periodEnd,omitempty"`
}

//encore:api auth method=POST path=/bills
func (s *Service) CreateBill(ctx context.Context, req *CreateBillRequest) (*CreateBillResponse, error) {
	currency := Currency(req.Currency)
	if !currency.Valid() {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: fmt.Sprintf("invalid currency: %s", req.Currency),
		}
	}
	if err := validatePeriod(req.PeriodStart, req.PeriodEnd); err != nil {
		return nil, err
	}

	accountID := callerAccountID(ctx)
	billID := newBillID(accountID, req.IdempotencyKey)
	workflowID := fmt.Sprintf("bill-%s", billID)

	input := BillWorkflowInput{
		BillID:      billID,
		AccountID:   accountID,
		Currency:    currency,
		PeriodStart: req.PeriodStart,
		PeriodEnd:   req.PeriodEnd,
	}

	_, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: TaskQueue,
		// Default reuse policy is AllowDuplicate, but the deterministic
		// workflow ID derived from the idempotency key means a retry
		// will hit the running workflow regardless. We surface
		// AlreadyStarted as a 200 with the existing bill state below.
	}, BillingWorkflow, input)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			// Idempotent retry: same account + key landed on the same
			// workflow ID. Return the existing bill state with 200.
			return s.createBillReplayResponse(ctx, billID, currency, req)
		}
		return nil, &errs.Error{
			Code:    errs.Unavailable,
			Message: fmt.Sprintf("failed to start bill workflow: %v", err),
		}
	}

	return &CreateBillResponse{
		BillID:      billID,
		Status:      string(BillStatusOpen),
		Currency:    string(currency),
		PeriodStart: req.PeriodStart,
		PeriodEnd:   req.PeriodEnd,
	}, nil
}

// idempotencyNamespace is a fixed namespace UUID used to derive bill
// IDs from (account, idempotency key). Generated once; do not change —
// changing this constant breaks idempotency for all existing keys.
var idempotencyNamespace = uuid.MustParse("0f7d8c4b-3a91-4f1b-9a6f-1a0c7d2e8f51")

// newBillID returns a bill UUID. With an idempotency key it is
// deterministic over (account, key) — same inputs always yield the
// same UUID, so a retried CreateBill maps to the same workflow ID
// and Temporal de-dupes. Without a key, returns a fresh random UUID.
func newBillID(accountID, idempotencyKey string) string {
	if idempotencyKey == "" {
		return uuid.NewString()
	}
	return uuid.NewSHA1(idempotencyNamespace, []byte(accountID+":"+idempotencyKey)).String()
}

// createBillReplayResponse builds the response for the idempotent
// retry path. The original CreateBill call already populated the
// workflow with its requested period/currency; the request that lost
// the race may have asked for different values, so we report the
// AUTHORITATIVE state (from the workflow query) rather than the
// caller's intent. If query fails, fall back to the caller's request
// shape so the response is at least well-formed.
func (s *Service) createBillReplayResponse(ctx context.Context, billID string, currency Currency, req *CreateBillRequest) (*CreateBillResponse, error) {
	state, err := s.loadBill(ctx, billID)
	if err == nil {
		return &CreateBillResponse{
			BillID:      state.ID,
			Status:      string(state.Status),
			Currency:    string(state.Currency),
			PeriodStart: state.PeriodStart,
			PeriodEnd:   state.PeriodEnd,
		}, nil
	}
	rlog.Warn("idempotent CreateBill retry: state load failed, returning request echo",
		"bill_id", billID, "err", err)
	return &CreateBillResponse{
		BillID:      billID,
		Status:      string(BillStatusOpen),
		Currency:    string(currency),
		PeriodStart: req.PeriodStart,
		PeriodEnd:   req.PeriodEnd,
	}, nil
}

// validatePeriod rejects obviously-bad period configurations. PeriodEnd
// in the past is allowed (the workflow fires the timer immediately and
// closes), which keeps backdated bookkeeping bills supportable.
func validatePeriod(start, end *time.Time) error {
	if start != nil && end != nil && !end.After(*start) {
		return &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "periodEnd must be after periodStart",
		}
	}
	return nil
}

type AddLineItemRequest struct {
	IdempotencyKey string          `json:"idempotencyKey"`
	Description    string          `json:"description"`
	Amount         decimal.Decimal `json:"amount"`
	Currency       string          `json:"currency"`
}

type AddLineItemResponse struct {
	ItemID    string          `json:"itemId"`
	BillTotal decimal.Decimal `json:"billTotal"`
	ItemCount int             `json:"itemCount"`
}

//encore:api auth method=POST path=/bills/:id/line-items
func (s *Service) AddLineItem(ctx context.Context, id string, req *AddLineItemRequest) (*AddLineItemResponse, error) {
	if req.Description == "" {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "description is required",
		}
	}
	if !req.Amount.IsPositive() {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "amount must be positive",
		}
	}
	currency := Currency(req.Currency)
	if !currency.Valid() {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: fmt.Sprintf("invalid currency: %s", req.Currency),
		}
	}

	itemID := req.IdempotencyKey
	if itemID == "" {
		itemID = uuid.New().String()
	}

	workflowID := fmt.Sprintf("bill-%s", id)

	in := AddLineItemInput{
		ItemID:          itemID,
		Description:     req.Description,
		Amount:          req.Amount,
		Currency:        currency,
		CallerAccountID: callerAccountID(ctx),
	}

	handle, err := s.temporalClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        emptyRunID,
		UpdateName:   UpdateAddLineItem,
		Args:         []interface{}{in},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, mapTemporalRPCError(err, "add item")
	}

	var result AddLineItemResult
	if err := handle.Get(ctx, &result); err != nil {
		return nil, classifyUpdateError(err)
	}

	return &AddLineItemResponse{
		ItemID:    result.ItemID,
		BillTotal: result.BillTotal,
		ItemCount: result.ItemCount,
	}, nil
}

// mapTemporalRPCError classifies an error returned by submitting a signal
// or update to Temporal. Only NotFound and "workflow already completed"
// indicate the bill is gone or closed — every other Temporal RPC failure
// is an infrastructure error and must not be reported to callers as a
// business-state error, or they may stop retrying on a transient outage.
func mapTemporalRPCError(err error, op string) *errs.Error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return &errs.Error{
			Code:    errs.NotFound,
			Message: fmt.Sprintf("cannot %s: bill not found or already closed", op),
		}
	}
	if errors.Is(err, context.Canceled) {
		return &errs.Error{
			Code:    errs.Canceled,
			Message: fmt.Sprintf("cannot %s: request canceled", op),
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &errs.Error{
			Code:    errs.DeadlineExceeded,
			Message: fmt.Sprintf("cannot %s: deadline exceeded", op),
		}
	}
	rlog.Error("temporal rpc failed", "op", op, "err", err)
	return &errs.Error{
		Code:    errs.Unavailable,
		Message: fmt.Sprintf("cannot %s: service unavailable", op),
	}
}

// classifyUpdateError maps the outcome of a Temporal update handle's Get
// onto Encore error codes. Validator rejections surface as
// ApplicationError; the error Type distinguishes a state conflict (the
// bill's locked-in currency does not match the item — 409
// FailedPrecondition) from a bad payload (everything else — 400
// InvalidArgument). Timeouts and canceled-by-server surface as
// DeadlineExceeded. Everything else is treated as Internal and logged so
// infrastructure failures are not silently presented to callers as
// validation errors.
func classifyUpdateError(err error) *errs.Error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case billNotFoundErrType:
			return &errs.Error{
				Code:    errs.NotFound,
				Message: appErr.Message(),
			}
		case currencyMismatchErrType:
			return &errs.Error{
				Code:    errs.FailedPrecondition,
				Message: appErr.Message(),
			}
		}
		return &errs.Error{
			Code:    errs.InvalidArgument,
			Message: appErr.Message(),
		}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return &errs.Error{
			Code:    errs.DeadlineExceeded,
			Message: "add item timed out",
		}
	}
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return &errs.Error{
			Code:    errs.Canceled,
			Message: "add item canceled",
		}
	}
	rlog.Error("temporal update failed", "err", err)
	return &errs.Error{
		Code:    errs.Internal,
		Message: "failed to add item",
	}
}

type CloseBillResponse struct {
	BillID      string          `json:"billId"`
	Status      string          `json:"status"`
	TotalAmount decimal.Decimal `json:"totalAmount"`
	Currency    string          `json:"currency"`
	LineItems   []LineItem      `json:"lineItems"`
	ClosedAt    *time.Time      `json:"closedAt,omitempty"`
	CloseReason CloseReason     `json:"closeReason,omitempty"`
}

//encore:api auth method=POST path=/bills/:id/close
//
// CloseBill is idempotent. If the workflow has already completed (e.g.
// the caller is retrying a successful close, or the period-end timer
// already fired), the endpoint returns the persisted bill with the
// same response shape as a first-time close. Only an unknown bill ID
// produces a 404.
func (s *Service) CloseBill(ctx context.Context, id string) (*CloseBillResponse, error) {
	if err := s.assertOwnsBill(ctx, id); err != nil {
		return nil, err
	}
	workflowID := fmt.Sprintf("bill-%s", id)

	err := s.temporalClient.SignalWorkflow(ctx, workflowID, emptyRunID, SignalCloseBill, CloseBillSignal{})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return s.closeBillFromDB(ctx, id)
		}
		return nil, mapTemporalRPCError(err, "close bill")
	}

	run := s.temporalClient.GetWorkflow(ctx, workflowID, emptyRunID)
	var result BillResult
	if err := run.Get(ctx, &result); err != nil {
		rlog.Error("close bill workflow failed", "bill_id", id, "err", err)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to close bill",
		}
	}

	return s.closeBillFromDB(ctx, id)
}

// closeBillFromDB builds a CloseBillResponse from the persisted row.
// Used both on the success path (after the workflow completed) and on
// the idempotent-retry path (Temporal NotFound → workflow gone but
// row already present). Maps no-rows to 404; other errors to 500.
func (s *Service) closeBillFromDB(ctx context.Context, id string) (*CloseBillResponse, error) {
	bill, err := s.getBillFromDB(ctx, id)
	if err != nil {
		if errors.Is(err, sqldb.ErrNoRows) {
			return nil, &errs.Error{
				Code:    errs.NotFound,
				Message: fmt.Sprintf("bill %s not found", id),
			}
		}
		rlog.Error("failed to load closed bill", "bill_id", id, "err", err)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to retrieve closed bill",
		}
	}

	return &CloseBillResponse{
		BillID:      bill.ID,
		Status:      string(bill.Status),
		TotalAmount: bill.TotalAmount,
		Currency:    string(bill.Currency),
		LineItems:   bill.LineItems,
		ClosedAt:    bill.ClosedAt,
		CloseReason: bill.CloseReason,
	}, nil
}

type GetBillResponse struct {
	Bill Bill `json:"bill"`
}

//encore:api auth method=GET path=/bills/:id
func (s *Service) GetBill(ctx context.Context, id string) (*GetBillResponse, error) {
	bill, err := s.loadBill(ctx, id)
	if err != nil {
		return nil, err
	}
	if bill.AccountID != callerAccountID(ctx) {
		// Return NotFound (not PermissionDenied) so existence does not
		// leak to non-owners — a tenancy-scoped 404 is the same answer
		// the caller would get for a typo'd ID.
		return nil, &errs.Error{
			Code:    errs.NotFound,
			Message: fmt.Sprintf("bill %s not found", id),
		}
	}
	return &GetBillResponse{Bill: bill}, nil
}

// loadBill resolves a bill ID to its current state via Temporal query
// (open bills) with a DB fallback (closed bills). Returns an
// errs.Error with the appropriate code; callers should not wrap.
//
// Only NotFound from Temporal triggers the DB fallback. A non-NotFound
// query error (Temporal unavailable, context canceled) must NOT
// degrade to a DB lookup or an in-flight bill that lives only in
// workflow memory would report as 404.
func (s *Service) loadBill(ctx context.Context, id string) (Bill, error) {
	workflowID := fmt.Sprintf("bill-%s", id)

	state, err := s.queryBillState(ctx, workflowID)
	if err == nil {
		return state, nil
	}

	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		rlog.Error("bill workflow query failed", "bill_id", id, "err", err)
		return Bill{}, &errs.Error{
			Code:    errs.Unavailable,
			Message: "failed to query bill",
		}
	}

	bill, dberr := s.getBillFromDB(ctx, id)
	if dberr != nil {
		if errors.Is(dberr, sqldb.ErrNoRows) {
			return Bill{}, &errs.Error{
				Code:    errs.NotFound,
				Message: fmt.Sprintf("bill %s not found", id),
			}
		}
		rlog.Error("bill DB load failed", "bill_id", id, "err", dberr)
		return Bill{}, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to load bill",
		}
	}
	return bill, nil
}

// assertOwnsBill loads the bill and verifies the caller owns it. Used
// as a pre-check on per-bill mutating endpoints. Returns NotFound
// (404) for both "bill does not exist" and "bill belongs to another
// account" so existence isn't leaked across tenants.
//
// Cost: one extra round-trip (Temporal query or DB read) per
// mutating call. Acceptable for the stub; a production design would
// either cache the account_id by bill_id locally or push the check
// into the workflow validator on AddLineItem.
func (s *Service) assertOwnsBill(ctx context.Context, id string) error {
	bill, err := s.loadBill(ctx, id)
	if err != nil {
		return err
	}
	if bill.AccountID != callerAccountID(ctx) {
		return &errs.Error{
			Code:    errs.NotFound,
			Message: fmt.Sprintf("bill %s not found", id),
		}
	}
	return nil
}

type ListBillsRequest struct {
	Limit  int `query:"limit"`
	Offset int `query:"offset"`
}

type ListBillsResponse struct {
	Bills  []Bill `json:"bills"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

//encore:api auth method=GET path=/bills
func (s *Service) ListBills(ctx context.Context, req *ListBillsRequest) (*ListBillsResponse, error) {
	if req != nil && (req.Limit < 0 || req.Offset < 0) {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "limit and offset must be non-negative",
		}
	}
	limit, offset := normalizeListPagination(req)

	rows, err := db.Query(ctx, `
		SELECT id, account_id, status, currency, total_amount, created_at, closed_at,
		       period_start, period_end, close_reason
		FROM bills
		WHERE account_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`, callerAccountID(ctx), limit, offset)
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to list bills",
		}
	}
	defer rows.Close()

	bills := []Bill{}
	for rows.Next() {
		b, err := scanBillRow(rows)
		if err != nil {
			return nil, &errs.Error{
				Code:    errs.Internal,
				Message: "failed to scan bill",
			}
		}
		b.LineItems = []LineItem{}
		bills = append(bills, b)
	}
	if err := rows.Err(); err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: fmt.Sprintf("iterating bills: %v", err),
		}
	}

	return &ListBillsResponse{Bills: bills, Limit: limit, Offset: offset}, nil
}

func normalizeListPagination(req *ListBillsRequest) (limit, offset int) {
	limit = defaultListLimit
	if req != nil {
		if req.Limit > 0 {
			limit = req.Limit
		}
		if req.Offset > 0 {
			offset = req.Offset
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return limit, offset
}

// rowScanner abstracts over *sqldb.Row and *sqldb.Rows so a single Bill
// scan path serves both QueryRow and Query callers. Keeping the column
// list in one place stops the two reads from drifting.
type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanBillRow(s rowScanner) (Bill, error) {
	var (
		b           Bill
		closeReason *string
	)
	if err := s.Scan(
		&b.ID, &b.AccountID, &b.Status, &b.Currency, &b.TotalAmount, &b.CreatedAt, &b.ClosedAt,
		&b.PeriodStart, &b.PeriodEnd, &closeReason,
	); err != nil {
		return Bill{}, err
	}
	if closeReason != nil {
		b.CloseReason = CloseReason(*closeReason)
	}
	return b, nil
}

func (s *Service) queryBillState(ctx context.Context, workflowID string) (Bill, error) {
	resp, err := s.temporalClient.QueryWorkflow(ctx, workflowID, emptyRunID, QueryBillState)
	if err != nil {
		return Bill{}, err
	}

	var bill Bill
	err = resp.Get(&bill)
	return bill, err
}

func (s *Service) getBillFromDB(ctx context.Context, id string) (Bill, error) {
	row := db.QueryRow(ctx, `
		SELECT id, account_id, status, currency, total_amount, created_at, closed_at,
		       period_start, period_end, close_reason
		FROM bills WHERE id = $1`, id)
	b, err := scanBillRow(row)
	if err != nil {
		return Bill{}, err
	}

	rows, err := db.Query(ctx, `
		SELECT id, description, amount, currency, created_at
		FROM line_items WHERE bill_id = $1 ORDER BY created_at`, id)
	if err != nil {
		return Bill{}, err
	}
	defer rows.Close()

	b.LineItems = []LineItem{}
	for rows.Next() {
		var item LineItem
		if err := rows.Scan(&item.ID, &item.Description, &item.Amount, &item.Currency, &item.CreatedAt); err != nil {
			return Bill{}, fmt.Errorf("scan line item: %w", err)
		}
		b.LineItems = append(b.LineItems, item)
	}
	if err := rows.Err(); err != nil {
		return Bill{}, fmt.Errorf("iterating line items: %w", err)
	}

	return b, nil
}
