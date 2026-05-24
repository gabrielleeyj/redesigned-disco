-- All seeded rows have a numeric ISO 4217 code, and the registry
-- loader has no path that would create a row without one. Tighten
-- the column to NOT NULL so the schema reflects reality and the
-- scan helper can drop its sql.NullInt32 handling.
ALTER TABLE currencies
    ALTER COLUMN numeric_code SET NOT NULL;
