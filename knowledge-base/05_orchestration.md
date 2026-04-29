# 05 - Orchestration

Orchestration coordinates refreshes, prediction, publishing, supervisor ticks,
retriever hooks, and learning side effects. The main class is
`orchestration/orchestrator.py::DeterministicOrchestrator`.

## Key Files

- `orchestration/orchestrator.py` - refresh cycle, source fetch, prediction persistence
- `orchestration/planner.py` - actionable and working horizon planning
- `orchestration/supervisor.py` - queue, scheduled, and event-mode ticks
- `orchestration/refresh_service.py` - refresh request persistence helpers
- `prediction/engine.py` - deterministic forecast calculation
- `prediction/resolution.py` - target/source truth resolution
- `prediction/weather_assessment.py` - weather-risk summary
- `prediction/scenarios.py` - likely/slower/busier scenarios
- `publish/policy.py` - published vs working state and notification decision
- `workflows/actuals.py` - actual logging and learning trigger sequence
- `workflows/retriever_hooks.py` - digest refresh hooks
- `learning/update.py` - learning-state updates from evaluations

## Refresh Cycle

`DeterministicOrchestrator.run_refresh_cycle(...)`:

1. Builds a `RefreshPlan` for 14 actionable dates plus working-horizon dates.
2. Runs external catalog refresh discovery.
3. Starts a `forecast_refresh_runs` row.
4. For each date/window, calls `refresh_with_stored_baseline`.
5. Loads the operator baseline from `operator_weekly_baselines`.
6. Fetches source payloads and connector truth.
7. Runs Signal Interpreter when available.
8. Normalizes signals.
9. Builds `PredictionContext` with learning state and source summaries.
10. Runs `prediction/engine.py::run_forecast`.
11. Runs Prediction Governor when available, otherwise deterministic governance.
12. Decides publication destination with `publish/policy.py`.
13. Persists runs, components, digests, weather assessment, scenarios, evidence, and forecast state.
14. Completes the refresh run and may fire retriever hooks.

Actionable dates are written to `published_forecast_state`; farther working dates
are written to `working_forecast_state`.

## Prediction Inputs

The deterministic engine uses:

- setup profile and location context
- stored weekly baseline for the service day group
- seasonal prior by month and neighborhood type
- weather and external normalized signals
- service plan and service-state risk
- confidence calibration
- weather signature and external scan learning
- source reliability
- prediction adaptation state
- component learning for reserved/outside-cover behavior
- optional Brooklyn/reference weather comparison

## Actuals and Learning

`workflows/actuals.py::record_actual_total_and_update`:

1. Writes or updates `operator_actuals`.
2. Evaluates the current published state into `prediction_evaluations`.
3. Updates component learning.
4. On first evaluation, updates source reliability, effect learning, confidence calibration, and prediction adaptation.
5. Updates service-state risk and baseline learning when eligible.
6. Runs Anomaly Explainer when eligible.

API actual submission also captures notes and runs retriever hooks.

## Supervisor

`SupervisorService` handles:

- queued operator refresh requests
- scheduled refresh windows from `config/settings.py`
- event-mode checks
- source-monitor checks

The FastAPI background loop is optional and controlled by
`STORMREADY_V3_BACKGROUND_SUPERVISOR`.

## Publish Rules

`publish/policy.py::decide_publication` compares the candidate with the current
state. It publishes actionable dates and creates notifications for material
changes, interval widening, confidence drops, posture changes, service-state
changes, or event-mode refreshes when inside notification horizon.

See also: [04_agents.md](04_agents.md), [06_data_layer.md](06_data_layer.md),
[07_external_world.md](07_external_world.md).
