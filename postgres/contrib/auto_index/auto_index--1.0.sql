-- Create the stats table in a shared schema
CREATE TABLE column_scan_stats (
    table_name  TEXT        NOT NULL,
    column_name TEXT        NOT NULL,
    scan_count  FLOAT8      NOT NULL DEFAULT 0,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, column_name)
);