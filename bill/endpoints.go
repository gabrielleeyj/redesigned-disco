package bill

import (
	"context"
	"errors"
	"fmt"

	"encore.dev/beta/errs"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
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
	IdempotencyKey string `json:"idempotencyKey"`
	Description    string `json:"description"`
	AmountMinor    int64  `json:"amountMinor"`
	Currency       string `json:"currency"`
}

type AddLineItemResponse struct {
	ItemID    string `json:"itemId"`
	BillTotal int64  `json:"billTotal"`
	ItemCount int    `json:"itemCount"`
}

//encore:api public method=POST path=/bills/:id/line-items
func (s *Service) AddLineItem(ctx context.Context, id string, req *AddLineItemRequest) (*AddLineItemResponse, error) {
	if req.Description == "" {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "description is required",
		}
	}
	if req.AmountMinor <= 0 {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "amountMinor must be positive",
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

	signal := AddLineItemSignal{
		ItemID:      itemID,
		Description: req.Description,
		AmountMinor: req.AmountMinor,
		Currency:    currency,
	}

	err := s.temporalClient.SignalWorkflow(ctx, workflowID, emptyRunID, SignalAddLineItem, signal)
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.FailedPrecondition,
			Message: "cannot add item: bill is closed or not found",
		}
	}

	state, err := s.queryBillState(ctx, workflowID)
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: fmt.Sprintf("item %s added but bill state unavailable: %v", itemID, err),
		}
	}

	return &AddLineItemResponse{
		ItemID:    itemID,
		BillTotal: state.TotalAmount,
		ItemCount: len(state.LineItems),
	}, nil
}

type CloseBillResponse struct {
	BillID      string     `json:"billId"`
	Status      string     `json:"status"`
	TotalAmount int64      `json:"totalAmount"`
	Currency    string     `json:"currency"`
	LineItems   []LineItem `json:"lineItems"`
	ClosedAt    string     `json:"closedAt"`
}

//encore:api public method=POST path=/bills/:id/close
func (s *Service) CloseBill(ctx context.Context, id string) (*CloseBillResponse, error) {
	workflowID := fmt.Sprintf("bill-%s", id)

	err := s.temporalClient.SignalWorkflow(ctx, workflowID, emptyRunID, SignalCloseBill, CloseBillSignal{})
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.FailedPrecondition,
			Message: "cannot close bill: already closed or not found",
		}
	}

	var result BillResult
	run := s.temporalClient.GetWorkflow(ctx, workflowID, emptyRunID)
	err = run.Get(ctx, &result)
	if err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to get bill result",
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
		SELECT id, description, amount_minor, currency, created_at
		FROM line_items WHERE bill_id = $1 ORDER BY created_at`, id)
	if err != nil {
		return Bill{}, err
	}
	defer rows.Close()

	b.LineItems = []LineItem{}
	for rows.Next() {
		var item LineItem
		var amountMinor int64
		var cur Currency
		if err := rows.Scan(&item.ID, &item.Description, &amountMinor, &cur, &item.CreatedAt); err != nil {
			return Bill{}, fmt.Errorf("scan line item: %w", err)
		}
		item.Amount = Money{Amount: amountMinor, Currency: cur}
		b.LineItems = append(b.LineItems, item)
	}
	if err := rows.Err(); err != nil {
		return Bill{}, fmt.Errorf("iterating line items: %w", err)
	}

	return b, nil
}
