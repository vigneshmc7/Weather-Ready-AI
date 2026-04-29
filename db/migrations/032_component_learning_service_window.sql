ALTER TABLE component_learning_state ADD COLUMN IF NOT EXISTS service_window TEXT DEFAULT 'dinner';

CREATE INDEX IF NOT EXISTS idx_component_learning_state_window
    ON component_learning_state(operator_id, service_window, component_name);
