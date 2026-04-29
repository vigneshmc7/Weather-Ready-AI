ALTER TABLE external_source_catalog ADD COLUMN IF NOT EXISTS runtime_status TEXT DEFAULT 'planned';
ALTER TABLE external_source_catalog ADD COLUMN IF NOT EXISTS last_check_status TEXT;
ALTER TABLE external_source_catalog ADD COLUMN IF NOT EXISTS last_check_at TIMESTAMP;
ALTER TABLE external_source_catalog ADD COLUMN IF NOT EXISTS last_check_details_json TEXT;

CREATE TABLE IF NOT EXISTS source_check_log (
    check_id BIGINT PRIMARY KEY,
    operator_id TEXT NOT NULL,
    refresh_run_id TEXT,
    source_name TEXT NOT NULL,
    source_class TEXT,
    check_mode TEXT NOT NULL DEFAULT 'live',
    status TEXT NOT NULL,
    findings_count INTEGER DEFAULT 0,
    used_count INTEGER DEFAULT 0,
    failure_reason TEXT,
    details_json TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(operator_id) REFERENCES operators(operator_id)
);

CREATE SEQUENCE IF NOT EXISTS source_check_log_seq START 1;
ALTER TABLE source_check_log ALTER COLUMN check_id SET DEFAULT nextval('source_check_log_seq');

CREATE INDEX IF NOT EXISTS idx_source_check_log_operator
    ON source_check_log(operator_id, checked_at DESC, source_name);
