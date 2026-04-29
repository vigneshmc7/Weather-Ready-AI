from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from datetime import UTC, date, datetime
from unittest.mock import patch

from stormready_v3.agents.base import AgentResult, AgentRole, AgentStatus
from stormready_v3.domain.enums import (
    ForecastRegime,
    HorizonMode,
    PredictionCase,
    RefreshReason,
    ServiceState,
    ServiceWindow,
)
from stormready_v3.domain.models import CandidateForecastState
from stormready_v3.orchestration.orchestrator import DeterministicOrchestrator
from stormready_v3.orchestration.planner import RefreshPlan
from stormready_v3.storage.db import Database
from stormready_v3.storage.repositories import AgentFrameworkRepository


class _FakeDispatcher:
    def __init__(self, result: AgentResult) -> None:
        self.result = result
        self.calls = 0

    def dispatch(self, ctx):  # noqa: ANN001
        self.calls += 1
        self.last_ctx = ctx
        return self.result


def _candidate(**overrides) -> CandidateForecastState:
    base = CandidateForecastState(
        operator_id="op1",
        service_date=date(2026, 4, 14),
        service_window=ServiceWindow.DINNER,
        target_name="total_covers",
        forecast_expected=112,
        forecast_low=98,
        forecast_high=126,
        confidence_tier="medium",
        posture="STABLE",
        service_state=ServiceState.NORMAL,
        service_state_reason=None,
        prediction_case=PredictionCase.BASIC_PROFILE,
        forecast_regime=ForecastRegime.EARLY_LEARNING,
        horizon_mode=HorizonMode.NEAR,
        top_drivers=["weather_risk", "walk_in_trend", "service_state_override"],
        major_uncertainties=["weather may still shift"],
        target_definition_confidence="medium",
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


class OrchestratorPredictionGovernorDispatchTests(unittest.TestCase):
    def _orchestrator(self) -> DeterministicOrchestrator:
        orchestrator = DeterministicOrchestrator.__new__(DeterministicOrchestrator)
        orchestrator.db = object()
        orchestrator._provider = object()
        orchestrator._agent_dispatcher = None
        orchestrator._prediction_governor_dispatcher = None
        return orchestrator

    def test_builds_dispatcher_when_none_is_injected(self) -> None:
        orchestrator = self._orchestrator()
        dispatcher = _FakeDispatcher(
            AgentResult(
                role=AgentRole.PREDICTION_GOVERNOR,
                run_id="run_pg",
                status=AgentStatus.OK,
                outputs=[{
                    "emphasized_driver_indices": [2, 0],
                    "clarification_needed": True,
                    "clarification_question": "Confirm service details.",
                    "uncertainty_notes": ["Weather may still shift."],
                    "governance_path": "ai",
                }],
            )
        )
        candidate = _candidate(source_prediction_run_id="pred_run_1")

        with patch("stormready_v3.orchestration.orchestrator.build_agent_dispatcher", return_value=dispatcher) as mocked:
            result = orchestrator._govern_prediction_candidate(
                candidate=candidate,
                learning_context={"baseline_history_depth": 7},
            )

        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(dispatcher.calls, 1)
        self.assertEqual(result.emphasized_driver_indices, [2, 0])
        self.assertTrue(result.clarification_needed)
        self.assertEqual(result.clarification_question, "Confirm service details.")
        self.assertEqual(result.governance_path, "ai")

    def test_uses_deterministic_output_when_dispatch_fails(self) -> None:
        orchestrator = self._orchestrator()
        dispatcher = _FakeDispatcher(
            AgentResult(
                role=AgentRole.PREDICTION_GOVERNOR,
                run_id="run_pg",
                status=AgentStatus.BLOCKED,
                blocked_reason="provider unavailable",
            )
        )
        candidate = _candidate(
            top_drivers=["baseline service window pattern", "weather_disruption_risk", "service_state_override"],
            service_state=ServiceState.PARTIAL,
            service_state_reason="suggestion from service notes",
            target_definition_confidence="low",
            confidence_tier="very_low",
            major_uncertainties=["bookings are sparse"],
        )

        with patch("stormready_v3.orchestration.orchestrator.build_agent_dispatcher", return_value=dispatcher):
            result = orchestrator._govern_prediction_candidate(
                candidate=candidate,
                learning_context={"baseline_history_depth": 1},
            )

        self.assertEqual(result.governance_path, "deterministic_base")
        self.assertEqual(result.emphasized_driver_indices, [2, 1, 0])
        self.assertTrue(result.clarification_needed)
        self.assertEqual(
            result.clarification_question,
            "The service state looks abnormal. Confirming whether service was limited will improve forecast reliability.",
        )
        self.assertIn("component truth is still developing", result.uncertainty_notes)
        self.assertIn("service state may still need operator confirmation", result.uncertainty_notes)

    def test_learning_context_includes_operator_facts_and_confirmed_hypotheses(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(db_path=Path(temp_dir) / "governor_learning.duckdb")
            db.initialize()
            db.execute(
                "INSERT INTO operators (operator_id, restaurant_name) VALUES (?, ?)",
                ["op1", "Test Bistro"],
            )
            db.execute(
                """
                INSERT INTO operator_fact_memory (
                    operator_id, fact_key, fact_value_json, confidence, provenance, source_ref, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "op1",
                    "patio::rain",
                    json.dumps({"effect": "rain trims patio covers"}),
                    "high",
                    "operator_confirmed",
                    "learning_agenda::rain",
                    "active",
                ],
            )
            db.execute(
                """
                INSERT INTO operator_hypothesis_state (
                    operator_id, hypothesis_key, status, confidence, hypothesis_value_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    "op1",
                    "rain_patio",
                    "confirmed",
                    "medium",
                    json.dumps({"proposition": "Rain trims patio covers."}),
                ],
            )
            orchestrator = DeterministicOrchestrator.__new__(DeterministicOrchestrator)
            orchestrator.agent_framework = AgentFrameworkRepository(db)
            context = SimpleNamespace(
                operator_profile=SimpleNamespace(operator_id="op1"),
                forecast_regime=ForecastRegime.EARLY_LEARNING,
                confidence_calibration={},
                weather_signature_learning={},
                source_reliability={},
                brooklyn_similarity_score=None,
                location_context=SimpleNamespace(
                    transit_relevance=False,
                    venue_relevance=False,
                    hotel_travel_relevance=False,
                ),
            )

            learning_context = orchestrator._build_learning_context_for_governor(context)
            db.close()

        self.assertEqual(learning_context["operator_facts"][0]["fact_key"], "patio::rain")
        self.assertEqual(learning_context["confirmed_hypotheses"][0]["hypothesis_key"], "rain_patio")

    def test_refresh_cycle_runs_current_and_temporal_retrievers(self) -> None:
        orchestrator = DeterministicOrchestrator.__new__(DeterministicOrchestrator)
        orchestrator.db = object()
        orchestrator._agent_dispatcher = object()
        orchestrator.external_catalog = SimpleNamespace(run_refresh_discovery=lambda **_kwargs: {"ok": True})
        orchestrator.utc_now = lambda: datetime(2026, 4, 14, 12, 0, tzinfo=UTC)
        orchestrator._start_refresh_run = lambda **_kwargs: "refresh_1"
        orchestrator._complete_refresh_run = lambda **_kwargs: None
        refreshed: list[tuple[date, ServiceWindow]] = []

        def fake_refresh_with_stored_baseline(**kwargs):  # noqa: ANN001
            refreshed.append((kwargs["service_date"], kwargs["service_window"]))

        orchestrator.refresh_with_stored_baseline = fake_refresh_with_stored_baseline
        plan = RefreshPlan(
            reason=RefreshReason.OPERATOR_REQUESTED,
            run_date=date(2026, 4, 14),
            actionable_dates=[date(2026, 4, 14)],
            working_dates=[],
            refresh_window="midday",
        )
        profile = SimpleNamespace(
            operator_id="op1",
            active_service_windows=[ServiceWindow.DINNER],
            primary_service_window=ServiceWindow.DINNER,
        )

        with (
            patch("stormready_v3.orchestration.orchestrator.plan_refresh_cycle", return_value=plan),
            patch("stormready_v3.workflows.retriever_hooks.run_retriever_hooks") as retriever_hooks,
        ):
            result = orchestrator.run_refresh_cycle(
                profile=profile,
                location_context=SimpleNamespace(),
                refresh_reason=RefreshReason.OPERATOR_REQUESTED,
                run_date=date(2026, 4, 14),
            )

        self.assertEqual(result, plan)
        self.assertEqual(refreshed, [(date(2026, 4, 14), ServiceWindow.DINNER)])
        retriever_hooks.assert_called_once()
        self.assertEqual(retriever_hooks.call_args.kwargs["kinds"], ("current_state", "temporal"))


if __name__ == "__main__":
    unittest.main()
