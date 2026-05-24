-- Append-only audit log. Every state-changing operation on a bill
-- writes one event. The trigger below blocks UPDATE and DELETE so the
-- log is tamper-evident at the database layer; superuser can still
-- override, but ordinary application connections cannot mutate
-- existing rows.
CREATE TABLE bill_events (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bill_id    UUID NOT NULL REFERENCES bills(id),
    kind       TEXT NOT NULL CHECK (kind IN ('OPENED', 'ITEM_ADDED', 'CLOSED')),
    actor      TEXT NOT NULL,
    payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_bill_events_bill_id_created_at
    ON bill_events(bill_id, created_at, id);

CREATE OR REPLACE FUNCTION bill_events_block_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bill_events is append-only — % rejected', TG_OP;
END;
$$;

CREATE TRIGGER bill_events_no_update
    BEFORE UPDATE ON bill_events
    FOR EACH ROW EXECUTE FUNCTION bill_events_block_mutation();

CREATE TRIGGER bill_events_no_delete
    BEFORE DELETE ON bill_events
    FOR EACH ROW EXECUTE FUNCTION bill_events_block_mutation();
