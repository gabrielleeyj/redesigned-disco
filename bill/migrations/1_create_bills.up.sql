CREATE TABLE bills (
    id           UUID PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED')),
    currency     TEXT NOT NULL CHECK (currency IN ('GEL', 'USD')),
    total_amount BIGINT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at    TIMESTAMPTZ
);

CREATE TABLE line_items (
    id           UUID PRIMARY KEY,
    bill_id      UUID NOT NULL REFERENCES bills(id),
    description  TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency     TEXT NOT NULL CHECK (currency IN ('GEL', 'USD')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_line_items_bill_id ON line_items(bill_id);
