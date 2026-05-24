ALTER TABLE bills
    ADD COLUMN period_start TIMESTAMPTZ,
    ADD COLUMN period_end   TIMESTAMPTZ,
    ADD COLUMN close_reason TEXT CHECK (close_reason IN ('SIGNAL', 'PERIOD_END'));

CREATE INDEX idx_bills_period_end ON bills(period_end) WHERE period_end IS NOT NULL;
