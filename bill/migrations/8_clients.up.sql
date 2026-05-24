-- Local stand-in for an external account / identity service. In a
-- microservice deployment the bills service would NOT own this
-- table; it would call out to an account service and trust the
-- response. We keep it local so the take-home is demoable end-to-end
-- with a single repo — the auth handler is annotated TODO production
-- accordingly.
CREATE TABLE clients (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO clients (id, name, status) VALUES
    ('acct-alpha',     'Alpha Demo Co',     'ACTIVE'),
    ('acct-beta',      'Beta Demo Ltd',     'ACTIVE'),
    ('acct-suspended', 'Suspended Demo SA', 'SUSPENDED');

-- Backfill any bills already in the table (e.g. dev-DB rows from
-- prior runs) so the FK can be added without conflict. Production
-- DBs would start from a clean schema; this is defensive for local
-- development.
INSERT INTO clients (id, name, status)
SELECT DISTINCT account_id, 'Legacy ' || account_id, 'ACTIVE'
FROM bills
WHERE account_id <> '' AND account_id NOT IN (SELECT id FROM clients);

DELETE FROM bill_events WHERE bill_id IN (SELECT id FROM bills WHERE account_id = '');
DELETE FROM line_items  WHERE bill_id IN (SELECT id FROM bills WHERE account_id = '');
DELETE FROM bills WHERE account_id = '';

ALTER TABLE bills
    ALTER COLUMN account_id DROP DEFAULT,
    ADD CONSTRAINT bills_account_fk FOREIGN KEY (account_id) REFERENCES clients(id);
