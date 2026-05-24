-- Stub tenancy: every bill belongs to an account. account_id is opaque
-- to this service; the bill API only enforces that the caller's account
-- (asserted by the auth handler) matches the bill's account on reads,
-- writes, and close.
ALTER TABLE bills
    ADD COLUMN account_id TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_bills_account_id_created_at
    ON bills(account_id, created_at DESC);

-- Drop the default once existing rows are backfilled; new bills MUST
-- carry an account_id from the auth handler. A separate down-migration
-- isn't needed for the take-home; documenting here.
