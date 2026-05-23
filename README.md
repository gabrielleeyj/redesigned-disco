# Pave Bank Fees API

A billing system built with [Encore](https://encore.dev) for service orchestration and [Temporal](https://temporal.io) for durable workflow management.

## Architecture

The bill lifecycle is modeled as a long-running Temporal workflow:

1. **CreateBill** starts a new workflow (one per bill)
2. **AddLineItem** signals the running workflow to accumulate items in memory
3. **CloseBill** signals the workflow to finalize, persist to DB, and complete
4. **GetBill** queries the workflow state (or DB if closed)

Temporal provides: sequential signal processing (no race conditions), crash recovery via event sourcing, and efficient long-running timers.

### Money Representation

All monetary amounts use `int64` minor units (cents for USD, tetri for GEL). No floating point arithmetic.

## Prerequisites

- [Encore CLI](https://encore.dev/docs/install) (`curl -L https://encore.dev/install.sh | bash`)
- [Docker](https://docs.docker.com/get-docker/) (for Temporal server + PostgreSQL)
- Go 1.22+

## Setup

```bash
# 1. Start Temporal server
docker compose up -d

# 2. Start the Encore app (auto-provisions PostgreSQL + runs migrations)
encore run

# 3. Access Temporal UI
open http://localhost:8233

# 4. Access Encore local dashboard
open http://localhost:9400
```

## API Reference

### Create Bill

```bash
curl -X POST http://localhost:4000/bills \
  -H "Content-Type: application/json" \
  -d '{"currency": "USD"}'
```

Response:
```json
{"billId": "uuid", "status": "OPEN", "currency": "USD"}
```

### Add Line Item

```bash
curl -X POST http://localhost:4000/bills/{id}/line-items \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "unique-key-1",
    "description": "Monthly service fee",
    "amountMinor": 1500,
    "currency": "USD"
  }'
```

Response:
```json
{"itemId": "unique-key-1", "billTotal": 1500, "itemCount": 1}
```

### Close Bill

```bash
curl -X POST http://localhost:4000/bills/{id}/close
```

Response:
```json
{
  "billId": "uuid",
  "status": "CLOSED",
  "totalAmount": 4000,
  "currency": "USD",
  "lineItems": [...],
  "closedAt": "2024-01-15T10:30:00Z"
}
```

### Get Bill

```bash
curl http://localhost:4000/bills/{id}
```

### List Bills (closed, from DB)

```bash
curl http://localhost:4000/bills
```

## Error Semantics

| Scenario | HTTP Status | Error Code |
|----------|-------------|------------|
| Invalid currency | 400 | `invalid_argument` |
| Missing required field | 400 | `invalid_argument` |
| Bill not found | 404 | `not_found` |
| Add item to closed bill | 409 | `failed_precondition` |
| Currency mismatch | 400 | `invalid_argument` |
| Temporal unavailable | 503 | `unavailable` |
| Duplicate bill creation | 409 | `already_exists` |

## Testing

```bash
# Run all tests
encore test ./bill/...

# With race detection
encore test -race ./bill/...

# With coverage
encore test -cover ./bill/...
```

## Design Decisions

- **One currency per bill** — line items must match the bill's currency. Cross-currency conversion is the caller's responsibility.
- **Idempotent line items** — duplicate `idempotencyKey` values are silently ignored (no double-counting).
- **ContinueAsNew at 1000 items** — protects against Temporal's 50K event history limit.
- **No DB writes until close** — all state lives in the workflow until finalization. This eliminates consistency issues between workflow state and DB state.
