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
}

type CreateBillResponse struct {
	BillID      string     `json:"billId"`
	Status      string     `json:"status"`
	Currency    string     `json:"currency"`
	PeriodStart *time.Time `json:"periodStart,omitempty"`
	PeriodEnd   *time.Time `json:"periodEnd,omitempty"`
}

//encore:api public method=POST path=/bills
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

	billID := uuid.New().String()
	workflowID := fmt.Sprintf("bill-%s", billID)

	input := BillWorkflowInput{
		BillID:      billID,
		Currency:    currency,
		PeriodStart: req.PeriodStart,
		PeriodEnd:   req.PeriodEnd,
	}

	_, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: TaskQueue,
	}, BillingWorkflow, input)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return nil, &errs.Error{
				Code:    errs.AlreadyExists,
				Message: "bill already exists",
			}
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

//encore:api public method=POST path=/bills/:id/line-items
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
		ItemID:      itemID,
		Description: req.Description,
		Amount:      req.Amount,
		Currency:    currency,
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
// onto Encore error codes. Validator rejections surface as ApplicationError
// (-> InvalidArgument). Timeouts and canceled-by-server surface as
// DeadlineExceeded. Everything else is treated as Internal and logged so
// infrastructure failures are not silently presented to callers as
// validation errors.
func classifyUpdateError(err error) *errs.Error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
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

//encore:api public method=POST path=/bills/:id/close
//
// CloseBill is idempotent. If the workflow has already completed (e.g.
// the caller is retrying a successful close, or the period-end timer
// already fired), the endpoint returns the persisted bill with the
// same response shape as a first-time close. Only an unknown bill ID
// produces a 404.
func (s *Service) CloseBill(ctx context.Context, id string) (*CloseBillResponse, error) {
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

//encore:api public method=GET path=/bills/:id
func (s *Service) GetBill(ctx context.Context, id string) (*GetBillResponse, error) {
	workflowID := fmt.Sprintf("bill-%s", id)

	state, err := s.queryBillState(ctx, workflowID)
	if err == nil {
		return &GetBillResponse{Bill: state}, nil
	}

	// Only fall back to the DB when Temporal tells us the workflow is gone
	// (NotFound). Open bills exist only in workflow memory until CloseBill
	// persists them — if the query failed for any other reason (Temporal
	// unavailable, context canceled, network blip) we'd report a false 404
	// for a live bill. Surface those as Unavailable instead.
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		rlog.Error("bill workflow query failed", "bill_id", id, "err", err)
		return nil, &errs.Error{
			Code:    errs.Unavailable,
			Message: "failed to query bill",
		}
	}

	bill, err := s.getBillFromDB(ctx, id)
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.NotFound,
			Message: fmt.Sprintf("bill %s not found", id),
		}
	}

	return &GetBillResponse{Bill: bill}, nil
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

//encore:api public method=GET path=/bills
func (s *Service) ListBills(ctx context.Context, req *ListBillsRequest) (*ListBillsResponse, error) {
	if req != nil && (req.Limit < 0 || req.Offset < 0) {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "limit and offset must be non-negative",
		}
	}
	limit, offset := normalizeListPagination(req)

	rows, err := db.Query(ctx, `
		SELECT id, status, currency, total_amount, created_at, closed_at,
		       period_start, period_end, close_reason
		FROM bills ORDER BY created_at DESC
		LIMIT $1 OFFSET $2`, limit, offset)
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
		&b.ID, &b.Status, &b.Currency, &b.TotalAmount, &b.CreatedAt, &b.ClosedAt,
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
		SELECT id, status, currency, total_amount, created_at, closed_at,
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
