CREATE TABLE currencies (
    code         TEXT NOT NULL PRIMARY KEY,
    name         TEXT NOT NULL,
    numeric_code INTEGER,
    minor_unit   INTEGER NOT NULL CHECK (minor_unit >= 0 AND minor_unit <= 10),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO currencies (code, name, numeric_code, minor_unit) VALUES
    ('USD', 'US Dollar',        840, 2),
    ('EUR', 'Euro',             978, 2),
    ('GBP', 'Pound Sterling',   826, 2),
    ('GEL', 'Lari',             981, 2),
    ('JPY', 'Yen',              392, 0),
    ('KRW', 'Won',              410, 0),
    ('BHD', 'Bahraini Dinar',    48, 3),
    ('KWD', 'Kuwaiti Dinar',    414, 3);

ALTER TABLE bills
    ADD CONSTRAINT bills_currency_fk
    FOREIGN KEY (currency) REFERENCES currencies(code);

ALTER TABLE line_items
    ADD CONSTRAINT line_items_currency_fk
    FOREIGN KEY (currency) REFERENCES currencies(code);
