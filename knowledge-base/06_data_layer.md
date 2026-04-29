# 06 - Data Layer

DuckDB is the runtime database. The default file is
`runtime_data/local/stormready_v3.duckdb`, overrideable with
`STORMREADY_V3_DB_PATH`.

`storage/db.py::Database.initialize()` applies SQL migrations from
`db/migrations` in filename order. Current migrations run from
`001_initial_schema.sql` through `034_operator_context_adjustment_log.sql`.

## Key Files

- `storage/db.py` - connection wrapper and migration runner
- `storage/repositories.py::OperatorRepository` - profile, baselines, plans, actuals
- `storage/repositories.py::ForecastRepository` - prediction and forecast state
- `storage/repositories.py::AgentFrameworkRepository` - facts, hypotheses, agent-derived signals
- `storage/repositories.py::OperatorContextDigestRepository` - current/temporal/setup digests
- `storage/repositories.py::ConversationMemoryRepository` - facts, observations, agenda, decisions

## Main Tables

### Operator Setup

- `operators` - identity, address, timezone, profile fields, patio fields
- `operator_weekly_baselines` - dinner baseline by day group
- `location_context_profile` - relevance flags and location sensitivity hints
- `setup_bootstrap_runs` - setup/bootstrap status
- `historical_cover_uploads` - reviewed uploaded history and normalized rows
- `operator_reference_assets` - trained/selected reference assets from uploaded history

### Forecast Runtime

- `forecast_refresh_runs` - refresh audit trail
- `prediction_runs` - per-date/window forecast run
- `prediction_components` - component forecast rows
- `engine_digest` - compact deterministic forecast explanation payload
- `weather_assessment_log` - persisted weather assessment
- `forecast_scenario_state` - likely/slower/busier scenarios
- `published_forecast_state` - actionable forecast cards
- `working_forecast_state` - farther-horizon forecast state
- `notification_events` - pending/operator notification events

### External Signals

- `weather_pulls` - weather payload history
- `external_signal_log` - normalized and agent-derived signals
- `external_source_catalog` - curated/discovered source inventory
- `external_source_governance` - governance provenance
- `source_check_log` - source fetch/check records
- `connector_truth_log` - connector-sourced truth snapshots

### Conversation and Memory

- `conversation_messages` - chat transcript
- `conversation_note_log` - captured operator notes and correction suggestions
- `operator_fact_memory` - active learned facts
- `operator_hypothesis_state` - open/confirmed/rejected hypotheses
- `operator_observation_log` - structured qualitative observations
- `learning_agenda` - operator-facing questions/reminders
- `learning_decision_log` - promotion/learning decisions
- `operator_context_digest` - current and temporal context snapshots for chat

### Learning State

- `baseline_learning_state`
- `confidence_calibration_state`
- `weather_signature_state`
- `external_scan_learning_state`
- `prediction_adaptation_state`
- `operator_context_adjustment_log`
- `service_state_risk_state`
- `source_reliability_state`
- `component_learning_state`

### Infra

- `schema_migrations`
- `supervisor_runtime`
- `refresh_request_queue`
- `agent_run_log`

## Repository Pattern

Most code accesses DuckDB through methods in `storage/repositories.py` or through
small workflow functions. API/service code may also use direct SQL for compact
workspace serialization. Migrations remain the source of truth for schema.

See also: [03_api_layer.md](03_api_layer.md), [04_agents.md](04_agents.md),
[05_orchestration.md](05_orchestration.md).
