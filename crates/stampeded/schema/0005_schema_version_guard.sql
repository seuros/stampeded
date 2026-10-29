-- stampeded 0.4 reads this and refuses a version above 3, rather than
-- mistaking this database for an unversioned one and rewriting it.
CREATE TABLE IF NOT EXISTS stampeded_schema (
    version     integer     NOT NULL CHECK (version > 0),
    changed_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS stampeded_schema_one_row ON stampeded_schema ((true));
DELETE FROM stampeded_schema;
INSERT INTO stampeded_schema (version) VALUES (5);
