package bill

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
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

// latestRunID is the conventional empty-string value passed to
// Temporal client methods to target the most recent run of a
// workflow ID — required when the workflow may have transitioned
// through ContinueAsNew. Defined as a typed constant so the meaning
// is clear at call sites; the literal "" reads as a bug.
const latestRunID = ""

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

// temporalOp enumerates the RPC operations the endpoint layer
// classifies. Using a typed enum (not a free-form string from the
// caller) means user-controlled input cannot end up in error messages
// returned to clients.
type temporalOp string

const (
	temporalOpAddItem   temporalOp = "add item"
	temporalOpCloseBill temporalOp = "close bill"
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
	state, err := s.loadBillSummary(ctx, billID)
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
		RunID:        latestRunID,
		UpdateName:   UpdateAddLineItem,
		Args:         []interface{}{in},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, mapTemporalRPCError(err, temporalOpAddItem)
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

// mapTemporalRPCError classifies an error returned by submitting a
// signal or update to Temporal. Only NotFound and "workflow already
// completed" indicate the bill is gone or closed — every other
// Temporal RPC failure is an infrastructure error and must not be
// reported to callers as a business-state error, or they may stop
// retrying on a transient outage.
//
// op is a typed enum (not a free-form caller string) so user input
// cannot end up in returned error messages.
func mapTemporalRPCError(err error, op temporalOp) *errs.Error {
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
	rlog.Error("temporal rpc failed", "op", string(op), "err", err)
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
		reason := appErr.Type()
		if reason == "" {
			reason = "InvalidInput"
		}
		lineItemValidatorRejectionTotal.With(validatorRejectionLabels{Reason: reason}).Increment()
		lineItemsAddedTotal.With(lineItemResultLabels{Result: "rejected"}).Increment()

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

	err := s.temporalClient.SignalWorkflow(ctx, workflowID, latestRunID, SignalCloseBill, CloseBillSignal{})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return s.closeBillFromDB(ctx, id)
		}
		return nil, mapTemporalRPCError(err, temporalOpCloseBill)
	}

	run := s.temporalClient.GetWorkflow(ctx, workflowID, latestRunID)
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

// closeBillFromDB builds a CloseBillResponse from the persisted row
// plus line items. Used both on the success path (after the workflow
// completed) and on the idempotent-retry path (Temporal NotFound →
// workflow gone but row already present). Maps no-rows to 404; other
// errors to 500.
func (s *Service) closeBillFromDB(ctx context.Context, id string) (*CloseBillResponse, error) {
	bill, err := s.getBillRowFromDB(ctx, id)
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
	items, ierr := s.fetchLineItems(ctx, id)
	if ierr != nil {
		rlog.Error("failed to load line items for close", "bill_id", id, "err", ierr)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to retrieve closed bill items",
		}
	}

	return &CloseBillResponse{
		BillID:      bill.ID,
		Status:      string(bill.Status),
		TotalAmount: bill.TotalAmount,
		Currency:    string(bill.Currency),
		LineItems:   items,
		ClosedAt:    bill.ClosedAt,
		CloseReason: bill.CloseReason,
	}, nil
}

type GetBillResponse struct {
	Bill Bill `json:"bill"`
}

//encore:api auth method=GET path=/bills/:id
func (s *Service) GetBill(ctx context.Context, id string) (*GetBillResponse, error) {
	// Check ownership against the summary before paying for the line
	// items fetch — saves a DB round-trip for non-owners. Returns
	// NotFound (not PermissionDenied) so existence does not leak to
	// non-owners.
	summary, err := s.loadBillSummary(ctx, id)
	if err != nil {
		return nil, err
	}
	if summary.AccountID != callerAccountID(ctx) {
		return nil, &errs.Error{
			Code:    errs.NotFound,
			Message: fmt.Sprintf("bill %s not found", id),
		}
	}
	items, ierr := s.fetchLineItems(ctx, id)
	if ierr != nil {
		rlog.Error("line items fetch failed", "bill_id", id, "err", ierr)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to load line items",
		}
	}
	summary.LineItems = items
	return &GetBillResponse{Bill: summary}, nil
}

// loadBillSummary resolves a bill ID to its summary state (no line
// items) via Temporal query first, DB fallback for closed bills.
// Returns an errs.Error with the appropriate code; callers should
// not wrap.
//
// Only NotFound from Temporal triggers the DB fallback. A
// non-NotFound query error (Temporal unavailable, context canceled)
// must NOT degrade to a DB lookup or an in-flight bill that lives
// only in workflow memory would report as 404.
//
// Use loadBill (which calls this then attaches items) when the
// response needs line items; use loadBillSummary directly for the
// ownership pre-check on mutating endpoints, which only needs the
// account ID.
func (s *Service) loadBillSummary(ctx context.Context, id string) (Bill, error) {
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

	bill, dberr := s.getBillRowFromDB(ctx, id)
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

// fetchLineItems loads every line item for a bill in created-at order.
// Used by GetBill / CloseBill for the inline list. The paginated
// ListLineItems endpoint takes a different code path with cursor +
// limit to bound response size.
func (s *Service) fetchLineItems(ctx context.Context, billID string) ([]LineItem, error) {
	rows, err := db.Query(ctx, `
		SELECT id, description, amount, currency, created_at
		FROM line_items
		WHERE bill_id = $1
		ORDER BY created_at, id`, billID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []LineItem{}
	for rows.Next() {
		var item LineItem
		if err := rows.Scan(&item.ID, &item.Description, &item.Amount, &item.Currency, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan line item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate line items: %w", err)
	}
	return items, nil
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
	bill, err := s.loadBillSummary(ctx, id)
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

type ListLineItemsRequest struct {
	// Cursor is opaque — pass back the NextCursor from the previous
	// response. Empty cursor starts from the beginning. Format is
	// implementation-defined and may change; callers must not parse.
	Cursor string `query:"cursor"`
	Limit  int    `query:"limit"`
}

type ListLineItemsResponse struct {
	Items      []LineItem `json:"items"`
	NextCursor string     `json:"nextCursor,omitempty"`
	Limit      int        `json:"limit"`
}

//encore:api auth method=GET path=/bills/:id/line-items
//
// ListLineItems returns a paginated view of a bill's items in
// created-at order. Use this instead of the inline LineItems on
// GetBill / CloseBill for bills that may hold thousands of items —
// the inline list is unbounded and intended for small bills.
func (s *Service) ListLineItems(ctx context.Context, id string, req *ListLineItemsRequest) (*ListLineItemsResponse, error) {
	if err := s.assertOwnsBill(ctx, id); err != nil {
		return nil, err
	}

	limit := defaultListLimit
	if req != nil {
		if req.Limit < 0 {
			return nil, &errs.Error{
				Code:    errs.InvalidArgument,
				Message: "limit must be non-negative",
			}
		}
		if req.Limit > 0 {
			limit = req.Limit
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	cursorTime, cursorID, err := decodeLineItemCursor(req)
	if err != nil {
		return nil, err
	}

	// Fetch one extra row so we can tell whether another page exists
	// without a separate COUNT — if we get limit+1 back, there's more.
	// First page (no cursor) uses a different predicate to avoid
	// asking Postgres to compare against an empty UUID literal.
	var rows *sqldb.Rows
	if cursorID == "" {
		rows, err = db.Query(ctx, `
			SELECT id, description, amount, currency, created_at
			FROM line_items
			WHERE bill_id = $1
			ORDER BY created_at, id
			LIMIT $2`, id, limit+1)
	} else {
		rows, err = db.Query(ctx, `
			SELECT id, description, amount, currency, created_at
			FROM line_items
			WHERE bill_id = $1
			  AND (created_at, id) > ($2, $3)
			ORDER BY created_at, id
			LIMIT $4`, id, cursorTime, cursorID, limit+1)
	}
	if err != nil {
		rlog.Error("list line items query failed", "bill_id", id, "err", err)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to list line items",
		}
	}
	defer rows.Close()

	items := []LineItem{}
	for rows.Next() {
		var item LineItem
		if err := rows.Scan(&item.ID, &item.Description, &item.Amount, &item.Currency, &item.CreatedAt); err != nil {
			return nil, &errs.Error{
				Code:    errs.Internal,
				Message: "failed to scan line item",
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: fmt.Sprintf("iterating line items: %v", err),
		}
	}

	var nextCursor string
	if len(items) > limit {
		last := items[limit-1]
		nextCursor = encodeLineItemCursor(last.CreatedAt, last.ID)
		items = items[:limit]
	}

	return &ListLineItemsResponse{
		Items:      items,
		NextCursor: nextCursor,
		Limit:      limit,
	}, nil
}

type ListBillEventsRequest struct {
	Cursor string `query:"cursor"`
	Limit  int    `query:"limit"`
}

type ListBillEventsResponse struct {
	Events     []BillEvent `json:"events"`
	NextCursor string      `json:"nextCursor,omitempty"`
	Limit      int         `json:"limit"`
}

//encore:api auth method=GET path=/bills/:id/events
//
// ListBillEvents returns the bill's append-only audit log in
// chronological order. The table is immutable at the DB layer
// (UPDATE/DELETE triggers); this endpoint is read-only.
func (s *Service) ListBillEvents(ctx context.Context, id string, req *ListBillEventsRequest) (*ListBillEventsResponse, error) {
	if err := s.assertOwnsBill(ctx, id); err != nil {
		return nil, err
	}

	limit := defaultListLimit
	if req != nil {
		if req.Limit < 0 {
			return nil, &errs.Error{
				Code:    errs.InvalidArgument,
				Message: "limit must be non-negative",
			}
		}
		if req.Limit > 0 {
			limit = req.Limit
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	var (
		cursorTime time.Time
		cursorID   string
	)
	if req != nil && req.Cursor != "" {
		ts, eid, err := decodeKeysetCursor(req.Cursor)
		if err != nil {
			return nil, err
		}
		cursorTime, cursorID = ts, eid
	}

	var rows *sqldb.Rows
	var err error
	if cursorID == "" {
		rows, err = db.Query(ctx, `
			SELECT id, bill_id, kind, actor, payload, created_at
			FROM bill_events
			WHERE bill_id = $1
			ORDER BY created_at, id
			LIMIT $2`, id, limit+1)
	} else {
		rows, err = db.Query(ctx, `
			SELECT id, bill_id, kind, actor, payload, created_at
			FROM bill_events
			WHERE bill_id = $1 AND (created_at, id) > ($2, $3)
			ORDER BY created_at, id
			LIMIT $4`, id, cursorTime, cursorID, limit+1)
	}
	if err != nil {
		rlog.Error("list bill events query failed", "bill_id", id, "err", err)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to list bill events",
		}
	}
	defer rows.Close()

	events := []BillEvent{}
	for rows.Next() {
		var e BillEvent
		var kind string
		if err := rows.Scan(&e.ID, &e.BillID, &kind, &e.Actor, &e.Payload, &e.CreatedAt); err != nil {
			return nil, &errs.Error{
				Code:    errs.Internal,
				Message: "failed to scan bill event",
			}
		}
		e.Kind = BillEventKind(kind)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: fmt.Sprintf("iterating bill events: %v", err),
		}
	}

	var nextCursor string
	if len(events) > limit {
		last := events[limit-1]
		nextCursor = encodeLineItemCursor(last.CreatedAt, last.ID)
		events = events[:limit]
	}

	return &ListBillEventsResponse{
		Events:     events,
		NextCursor: nextCursor,
		Limit:      limit,
	}, nil
}

func decodeLineItemCursor(req *ListLineItemsRequest) (time.Time, string, error) {
	if req == nil || req.Cursor == "" {
		return time.Time{}, "", nil
	}
	return decodeKeysetCursor(req.Cursor)
}

func encodeLineItemCursor(ts time.Time, id string) string {
	return base64.URLEncoding.EncodeToString([]byte(ts.Format(time.RFC3339Nano) + "|" + id))
}

type ListBillsRequest struct {
	// Status filters by lifecycle state. Empty returns OPEN+CLOSED.
	// Valid values: OPEN, CLOSED.
	Status string `query:"status"`
	// Cursor is opaque — pass back NextCursor from the previous
	// response. Format is implementation-defined.
	Cursor string `query:"cursor"`
	Limit  int    `query:"limit"`
}

type ListBillsResponse struct {
	Bills      []Bill `json:"bills"`
	NextCursor string `json:"nextCursor,omitempty"`
	Limit      int    `json:"limit"`
}

//encore:api auth method=GET path=/bills
//
// ListBills returns bills owned by the caller in descending created-at
// order. Use the optional status filter and the NextCursor in the
// response for keyset pagination — OFFSET-based pagination is not
// supported because it scans linearly as the table grows.
func (s *Service) ListBills(ctx context.Context, req *ListBillsRequest) (*ListBillsResponse, error) {
	if req != nil && req.Limit < 0 {
		return nil, &errs.Error{
			Code:    errs.InvalidArgument,
			Message: "limit must be non-negative",
		}
	}

	limit := defaultListLimit
	if req != nil && req.Limit > 0 {
		limit = req.Limit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	statusFilter, err := normalizeListStatus(req)
	if err != nil {
		return nil, err
	}

	cursorTime, cursorID, err := decodeBillCursor(req)
	if err != nil {
		return nil, err
	}

	rows, err := queryBillsPage(ctx, callerAccountID(ctx), statusFilter, cursorTime, cursorID, limit+1)
	if err != nil {
		rlog.Error("list bills query failed", "err", err)
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
		bills = append(bills, b)
	}
	if err := rows.Err(); err != nil {
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: fmt.Sprintf("iterating bills: %v", err),
		}
	}

	var nextCursor string
	if len(bills) > limit {
		last := bills[limit-1]
		nextCursor = encodeBillCursor(last.CreatedAt, last.ID)
		bills = bills[:limit]
	}

	return &ListBillsResponse{Bills: bills, NextCursor: nextCursor, Limit: limit}, nil
}

// normalizeListStatus accepts OPEN, CLOSED, or empty. Empty returns
// the wildcard sentinel ("") consumed by queryBillsPage. Anything
// else is an InvalidArgument.
func normalizeListStatus(req *ListBillsRequest) (string, error) {
	if req == nil || req.Status == "" {
		return "", nil
	}
	switch BillStatus(req.Status) {
	case BillStatusOpen, BillStatusClosed:
		return req.Status, nil
	}
	return "", &errs.Error{
		Code:    errs.InvalidArgument,
		Message: fmt.Sprintf("status must be OPEN or CLOSED (got %q)", req.Status),
	}
}

// queryBillsPage assembles the keyset-paginated query. Branches on
// cursor presence (empty UUID can't be cast for tuple comparison) and
// status presence. Sort is (created_at DESC, id DESC) so the
// `(created_at, id) < (cursor)` predicate slices the next page.
func queryBillsPage(ctx context.Context, accountID, status string, cursorTime time.Time, cursorID string, limit int) (*sqldb.Rows, error) {
	const cols = `id, account_id, status, currency, total_amount, created_at, closed_at,
	              period_start, period_end, close_reason`
	switch {
	case cursorID == "" && status == "":
		return db.Query(ctx, `
			SELECT `+cols+` FROM bills
			WHERE account_id = $1
			ORDER BY created_at DESC, id DESC
			LIMIT $2`, accountID, limit)
	case cursorID == "" && status != "":
		return db.Query(ctx, `
			SELECT `+cols+` FROM bills
			WHERE account_id = $1 AND status = $2
			ORDER BY created_at DESC, id DESC
			LIMIT $3`, accountID, status, limit)
	case cursorID != "" && status == "":
		return db.Query(ctx, `
			SELECT `+cols+` FROM bills
			WHERE account_id = $1 AND (created_at, id) < ($2, $3)
			ORDER BY created_at DESC, id DESC
			LIMIT $4`, accountID, cursorTime, cursorID, limit)
	default:
		return db.Query(ctx, `
			SELECT `+cols+` FROM bills
			WHERE account_id = $1 AND status = $2 AND (created_at, id) < ($3, $4)
			ORDER BY created_at DESC, id DESC
			LIMIT $5`, accountID, status, cursorTime, cursorID, limit)
	}
}

func decodeBillCursor(req *ListBillsRequest) (time.Time, string, error) {
	if req == nil || req.Cursor == "" {
		return time.Time{}, "", nil
	}
	return decodeKeysetCursor(req.Cursor)
}

func encodeBillCursor(ts time.Time, id string) string {
	return base64.URLEncoding.EncodeToString([]byte(ts.Format(time.RFC3339Nano) + "|" + id))
}

// decodeKeysetCursor is shared between bill and line-item cursors —
// both use the same `<rfc3339nano>|<id>` shape. Kept private to this
// package; the cursor format is not part of the public contract.
func decodeKeysetCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", &errs.Error{Code: errs.InvalidArgument, Message: "invalid cursor"}
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", &errs.Error{Code: errs.InvalidArgument, Message: "invalid cursor"}
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", &errs.Error{Code: errs.InvalidArgument, Message: "invalid cursor"}
	}
	return ts, parts[1], nil
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
	resp, err := s.temporalClient.QueryWorkflow(ctx, workflowID, latestRunID, QueryBillState)
	if err != nil {
		return Bill{}, err
	}

	var bill Bill
	err = resp.Get(&bill)
	return bill, err
}

// getBillRowFromDB returns only the bills-table fields (no line
// items). Used by the loadBillSummary path and the close idempotency
// path. Callers that need items should join via fetchLineItems.
func (s *Service) getBillRowFromDB(ctx context.Context, id string) (Bill, error) {
	row := db.QueryRow(ctx, `
		SELECT id, account_id, status, currency, total_amount, created_at, closed_at,
		       period_start, period_end, close_reason
		FROM bills WHERE id = $1`, id)
	return scanBillRow(row)
}
