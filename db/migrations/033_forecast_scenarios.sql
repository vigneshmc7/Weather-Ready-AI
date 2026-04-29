CREATE TABLE IF NOT EXISTS forecast_scenario_state (
    scenario_id BIGINT PRIMARY KEY,
    prediction_run_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    service_date DATE NOT NULL,
    service_window TEXT NOT NULL,
    scenarios_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(operator_id) REFERENCES operators(operator_id)
);

CREATE SEQUENCE IF NOT EXISTS forecast_scenario_state_seq START 1;
ALTER TABLE forecast_scenario_state ALTER COLUMN scenario_id SET DEFAULT nextval('forecast_scenario_state_seq');

CREATE INDEX IF NOT EXISTS idx_forecast_scenario_state_run
    ON forecast_scenario_state(prediction_run_id);
