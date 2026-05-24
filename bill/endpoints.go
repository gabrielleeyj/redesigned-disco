package bill

import (
	"context"
	"errors"
	"fmt"

	"encore.dev/beta/errs"
	"encore.dev/rlog"
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
}

type CreateBillResponse struct {
	BillID   string `json:"billId"`
	Status   string `json:"status"`
	Currency string `json:"currency"`
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

	billID := uuid.New().String()
	workflowID := fmt.Sprintf("bill-%s", billID)

	input := BillWorkflowInput{
		BillID:   billID,
		Currency: currency,
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
		BillID:   billID,
		Status:   string(BillStatusOpen),
		Currency: string(currency),
	}, nil
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
	ClosedAt    string          `json:"closedAt"`
}

//encore:api public method=POST path=/bills/:id/close
func (s *Service) CloseBill(ctx context.Context, id string) (*CloseBillResponse, error) {
	workflowID := fmt.Sprintf("bill-%s", id)

	if err := s.temporalClient.SignalWorkflow(ctx, workflowID, emptyRunID, SignalCloseBill, CloseBillSignal{}); err != nil {
		return nil, mapTemporalRPCError(err, "close bill")
	}

	var result BillResult
	run := s.temporalClient.GetWorkflow(ctx, workflowID, emptyRunID)
	if err := run.Get(ctx, &result); err != nil {
		rlog.Error("close bill workflow failed", "bill_id", id, "err", err)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to close bill",
		}
	}

	bill, err := s.getBillFromDB(ctx, id)
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to retrieve closed bill",
		}
	}

	closedAt := ""
	if bill.ClosedAt != nil {
		closedAt = bill.ClosedAt.Format("2006-01-02T15:04:05Z07:00")
	}

	return &CloseBillResponse{
		BillID:      bill.ID,
		Status:      string(bill.Status),
		TotalAmount: bill.TotalAmount,
		Currency:    string(bill.Currency),
		LineItems:   bill.LineItems,
		ClosedAt:    closedAt,
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
		SELECT id, status, currency, total_amount, created_at, closed_at
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
		var b Bill
		if err := rows.Scan(&b.ID, &b.Status, &b.Currency, &b.TotalAmount, &b.CreatedAt, &b.ClosedAt); err != nil {
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
	var b Bill
	err := db.QueryRow(ctx, `
		SELECT id, status, currency, total_amount, created_at, closed_at
		FROM bills WHERE id = $1`, id).Scan(
		&b.ID, &b.Status, &b.Currency, &b.TotalAmount, &b.CreatedAt, &b.ClosedAt,
	)
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
