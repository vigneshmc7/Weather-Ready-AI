CREATE TABLE IF NOT EXISTS weather_assessment_log (
    assessment_id BIGINT PRIMARY KEY,
    prediction_run_id TEXT,
    operator_id TEXT NOT NULL,
    service_date DATE NOT NULL,
    service_window TEXT NOT NULL,
    assessment_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(operator_id) REFERENCES operators(operator_id)
);

CREATE SEQUENCE IF NOT EXISTS weather_assessment_log_seq START 1;
ALTER TABLE weather_assessment_log ALTER COLUMN assessment_id SET DEFAULT nextval('weather_assessment_log_seq');

CREATE INDEX IF NOT EXISTS idx_weather_assessment_log_operator
    ON weather_assessment_log(operator_id, service_date, service_window, created_at);
