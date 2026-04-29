CREATE TABLE IF NOT EXISTS operator_context_adjustment_log (
    adjustment_id BIGINT PRIMARY KEY,
    operator_id TEXT NOT NULL,
    prediction_run_id TEXT NOT NULL,
    service_window TEXT NOT NULL,
    horizon_mode TEXT,
    residual_pct DOUBLE NOT NULL,
    observation DOUBLE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(operator_id) REFERENCES operators(operator_id)
);

CREATE SEQUENCE IF NOT EXISTS operator_context_adjustment_log_seq START 1;
ALTER TABLE operator_context_adjustment_log ALTER COLUMN adjustment_id SET DEFAULT nextval('operator_context_adjustment_log_seq');

CREATE INDEX IF NOT EXISTS idx_operator_context_adjustment_log_run
    ON operator_context_adjustment_log(operator_id, prediction_run_id);
