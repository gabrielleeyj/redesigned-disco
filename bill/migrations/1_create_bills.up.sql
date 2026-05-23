CREATE TABLE bills (
    id           UUID PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED')),
    currency     TEXT NOT NULL,
    total_amount NUMERIC(30, 10) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at    TIMESTAMPTZ
);

CREATE TABLE line_items (
    id           UUID PRIMARY KEY,
    bill_id      UUID NOT NULL REFERENCES bills(id),
    description  TEXT NOT NULL,
    amount       NUMERIC(30, 10) NOT NULL CHECK (amount > 0),
    currency     TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_line_items_bill_id ON line_items(bill_id);
