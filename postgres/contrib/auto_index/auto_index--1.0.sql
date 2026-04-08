CREATE TABLE IF NOT EXISTS column_scan_stats (
    table_name TEXT,
    column_name TEXT,
    scan_count INT,
    PRIMARY KEY (table_name, column_name)
);