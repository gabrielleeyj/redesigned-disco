package bill

import "encore.dev/metrics"

// Metrics emitted by the bill service. Names follow the
// `<service>_<entity>_<action>_<unit>` convention; labels are kept
// low-cardinality so the time series count stays bounded.
//
// Emission points:
//   - bills_opened_total       — CreateBillActivity, on first insert only
//   - bills_closed_total       — CloseBillActivity, labeled by close reason
//   - line_items_added_total   — AppendLineItemActivity (accepted) and
//                                classifyUpdateError (rejected/duplicate)
//   - line_item_validator_rejection_total — classifyUpdateError
//
// Workflows do NOT emit metrics directly — metric calls are side
// effects and would re-trigger on replay. Endpoints and activities
// are the only producers.

type currencyLabels struct {
	Currency string
}

type closeReasonLabels struct {
	Reason string
}

type lineItemResultLabels struct {
	// Result is one of: accepted, duplicate, rejected.
	Result string
}

type validatorRejectionLabels struct {
	// Reason mirrors the ApplicationError type emitted by the
	// workflow validator (BillNotFound, CurrencyMismatch, ...). New
	// types should be added here so the dashboard stays consistent.
	Reason string
}

var (
	billsOpenedTotal = metrics.NewCounterGroup[currencyLabels, uint64](
		"bills_opened_total",
		metrics.CounterConfig{},
	)

	billsClosedTotal = metrics.NewCounterGroup[closeReasonLabels, uint64](
		"bills_closed_total",
		metrics.CounterConfig{},
	)

	lineItemsAddedTotal = metrics.NewCounterGroup[lineItemResultLabels, uint64](
		"line_items_added_total",
		metrics.CounterConfig{},
	)

	lineItemValidatorRejectionTotal = metrics.NewCounterGroup[validatorRejectionLabels, uint64](
		"line_item_validator_rejection_total",
		metrics.CounterConfig{},
	)
)

// Note: endpoint latency (incl. AddLineItem p50/p99) is already
// emitted by Encore's built-in request metrics, so we don't define a
// separate add_line_item_seconds histogram here. The custom counters
// above capture business-level signal that Encore's framework
// instrumentation can't infer (currency mix, close reasons,
// duplicate/rejected items).
