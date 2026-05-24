-- Supports keyset pagination over line items by (created_at, id).
CREATE INDEX idx_line_items_bill_id_created_at
    ON line_items(bill_id, created_at, id);
