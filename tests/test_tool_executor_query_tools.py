from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
import json
import tempfile
import unittest
from typing import Any

from stormready_v3.agents.base import AgentContext, AgentResult, AgentStatus
from stormready_v3.agents.tools import ToolExecutor
from stormready_v3.agents.unified import UnifiedAgentService
from stormready_v3.domain.enums import OnboardingState, ServiceState, ServiceWindow
from stormready_v3.domain.models import OperatorProfile
from stormready_v3.storage.db import Database
from stormready_v3.storage.repositories import OperatorContextDigestRepository, OperatorRepository


class FakeProvider:
    def is_available(self) -> bool:
        return True


class RecordingDispatcher:
    def __init__(self, first_output: dict[str, Any], second_output: dict[str, Any]) -> None:
        self.first_output = first_output
        self.second_output = second_output
        self.calls: list[AgentContext] = []

    def dispatch(self, ctx: AgentContext) -> AgentResult:
        self.calls.append(ctx)
        output = self.first_output if len(self.calls) == 1 else self.second_output
        return AgentResult(
            role=ctx.role,
            run_id=ctx.run_id,
            status=AgentStatus.OK,
            outputs=[output],
        )


class ToolExecutorQueryToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._temp_dir.name) / "query_tools.duckdb"
        self.db = Database(db_path=self.db_path)
        self.db.initialize()
        OperatorRepository(self.db).upsert_operator(
            OperatorProfile(
                operator_id="test_operator",
                restaurant_name="Test Bistro",
                canonical_address="10 Main St",
                timezone="America/New_York",
                onboarding_state=OnboardingState.COLD_START_READY,
            )
        )
        self.executor = ToolExecutor(self.db, reference_date=date(2026, 4, 14))

    def tearDown(self) -> None:
        self.db.close()
        self._temp_dir.cleanup()

    def test_query_forecast_detail_wraps_forecast_explanation(self) -> None:
        self._insert_forecast()

        result = self.executor.execute(
            "test_operator",
            "query_forecast_detail",
            {"service_date": "2026-04-14"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.tool_name, "query_forecast_detail")
        self.assertEqual(result.data["forecast_expected"], 112)
        self.assertEqual(result.data["top_drivers"], ["weather_risk"])
        self.assertEqual(result.data["weather_context"]["condition"], "heavy rain")
        self.assertEqual(result.data["weather_context"]["precip_chance_pct"], 54)
        self.assertTrue(result.data["weather_context"]["dinner_overlap"])
        self.assertEqual(result.data["weather_context"]["weather_effect_pct"], -31)

    def test_query_service_weather_returns_dinner_rain_context(self) -> None:
        self._insert_forecast()

        result = self.executor.execute(
            "test_operator",
            "query_service_weather",
            {"service_date": "2026-04-14"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.tool_name, "query_service_weather")
        self.assertEqual(result.data["weather"]["condition"], "heavy rain")
        self.assertEqual(result.data["weather"]["precip_chance_pct"], 54)
        self.assertTrue(result.data["weather"]["dinner_overlap"])

    def test_query_forecast_card_context_excludes_range_context(self) -> None:
        self._insert_forecast()

        result = self.executor.execute(
            "test_operator",
            "query_forecast_card_context",
            {"service_date": "2026-04-14"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.data["forecast_expected"], 112)
        self.assertEqual(result.data["scenario"], "Steady")
        self.assertIn("weather_context", result.data)
        self.assertNotIn("range", result.data)
        self.assertNotIn("forecast_low", result.data)
        self.assertNotIn("forecast_high", result.data)

    def test_query_forecast_why_returns_compact_reason_packet(self) -> None:
        self._insert_forecast()

        result = self.executor.execute(
            "test_operator",
            "query_forecast_why",
            {"service_date": "2026-04-14"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.tool_name, "query_forecast_why")
        self.assertEqual(result.data["forecast_expected"], 112)
        self.assertEqual(result.data["baseline"], 100)
        self.assertEqual(result.data["weather_context"]["precip_chance_pct"], 54)
        self.assertIn(
            {"component": "weather", "label": "weather", "effect_pct": -31, "direction": "lowers"},
            result.data["component_effects"],
        )
        self.assertNotIn("range", result.data)
        self.assertNotIn("forecast_low", result.data)
        self.assertNotIn("forecast_high", result.data)

    def test_query_hypothesis_backlog_filters_status(self) -> None:
        self.db.execute(
            """
            INSERT INTO operator_hypothesis_state (
                operator_id, hypothesis_key, status, confidence,
                hypothesis_value_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)
            """,
            [
                "test_operator",
                "rain_patio",
                "open",
                "medium",
                json.dumps({"proposition": "Rain trims patio covers."}),
                json.dumps({"source": "test"}),
                "test_operator",
                "old_event",
                "rejected",
                "low",
                json.dumps({"proposition": "Old event effect."}),
                json.dumps({}),
            ],
        )

        result = self.executor.execute(
            "test_operator",
            "query_hypothesis_backlog",
            {"status": "open"},
        )

        self.assertTrue(result.success)
        self.assertEqual([item["hypothesis_key"] for item in result.data["hypotheses"]], ["rain_patio"])

    def test_query_learning_state_filters_requested_area(self) -> None:
        self.db.execute(
            """
            INSERT INTO baseline_learning_state (
                operator_id, service_window, day_group, baseline_mid, baseline_variability, history_depth
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ["test_operator", ServiceWindow.DINNER.value, "fri", 118.0, 9.0, 12],
        )

        result = self.executor.execute(
            "test_operator",
            "query_learning_state",
            {"cascade": "baseline"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.data["cascade"], "baseline")
        self.assertEqual(result.data["baseline_learning"][0]["day_group"], "fri")

    def test_query_actuals_history_returns_actuals_with_eval(self) -> None:
        self._insert_prediction_run("run_actual", service_date="2026-04-13")
        self.db.execute(
            """
            INSERT INTO operator_actuals (
                operator_id, service_date, service_window, realized_total_covers,
                service_state, entry_mode, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "test_operator",
                date.fromisoformat("2026-04-13"),
                ServiceWindow.DINNER.value,
                101,
                ServiceState.NORMAL.value,
                "manual_structured",
                "Normal service.",
            ],
        )
        self.db.execute(
            """
            INSERT INTO prediction_evaluations (
                prediction_run_id, operator_id, service_date, service_window,
                actual_total_covers, forecast_expected, forecast_low, forecast_high,
                error_abs, error_pct, inside_interval, directional_bucket_correct,
                service_state_learning_eligibility
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "run_actual",
                "test_operator",
                date.fromisoformat("2026-04-13"),
                ServiceWindow.DINNER.value,
                101,
                110,
                98,
                122,
                9,
                -0.0818,
                True,
                True,
                "eligible",
            ],
        )

        result = self.executor.execute(
            "test_operator",
            "query_actuals_history",
            {"limit": 5, "state_filter": ServiceState.NORMAL.value},
        )

        self.assertTrue(result.success)
        actual = result.data["actuals"][0]
        self.assertEqual(actual["realized_total_covers"], 101)
        self.assertEqual(actual["forecast_expected"], 110)
        self.assertAlmostEqual(actual["error_pct"], -0.0818)

    def test_query_recent_signals_filters_dependency_group(self) -> None:
        self.db.execute(
            """
            INSERT INTO external_signal_log (
                signal_id, operator_id, signal_type, source_name, source_class,
                dependency_group, direction, strength, recommended_role,
                details_json, source_bucket, status, origin_agent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "sig1",
                "test_operator",
                "transit_disruption",
                "metro",
                "transit_disruption",
                "access",
                "down",
                0.03,
                "numeric_mover",
                json.dumps({"short_label": "Metro delay"}),
                "curated_local",
                "observed",
                None,
            ],
        )

        result = self.executor.execute(
            "test_operator",
            "query_recent_signals",
            {"dependency_group": "access"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.data["signals"][0]["signal_id"], "sig1")
        self.assertEqual(result.data["signals"][0]["details"]["short_label"], "Metro delay")

    def test_unified_chat_executes_query_tool_name(self) -> None:
        self._insert_forecast()
        digest_repo = OperatorContextDigestRepository(self.db)
        digest_repo.insert_digest(
            operator_id="test_operator",
            kind="current_state",
            produced_at=datetime.now(UTC),
            payload_json=json.dumps({
                "reference_date": "2026-04-14",
                "phase": "operations",
                "headline_forecast": {"service_date": "2026-04-14", "expected": 112},
            }),
            source_hash="current",
        )
        digest_repo.insert_digest(
            operator_id="test_operator",
            kind="temporal",
            produced_at=datetime.now(UTC),
            payload_json=json.dumps({"conversation_state": "active"}),
            source_hash="temporal",
        )
        dispatcher = RecordingDispatcher(
            first_output={
                "text": "Let me pull that.",
                "tool_calls": [
                    {"name": "query_forecast_detail", "arguments": {"service_date": "2026-04-14"}}
                ],
                "suggested_messages": [],
            },
            second_output={
                "text": "Dinner is at 112 covers.",
                "tool_calls": [],
                "suggested_messages": [],
            },
        )
        service = UnifiedAgentService(self.db, provider=FakeProvider(), agent_dispatcher=dispatcher)  # type: ignore[arg-type]

        response = service.respond(
            operator_id="test_operator",
            message="Show me tonight's breakdown.",
            reference_date=date(2026, 4, 14),
        )

        self.assertEqual(response.text, "Dinner is at 112 covers.")
        followup_payload = dict(dispatcher.calls[1].payload)
        self.assertEqual(followup_payload["tool_results"][0]["tool"], "query_forecast_detail")
        self.assertEqual(followup_payload["tool_results"][0]["data"]["forecast_expected"], 112)
        self.assertNotIn("range", followup_payload["tool_results"][0]["data"])
        self.assertNotIn("low", followup_payload["tool_results"][0]["data"])
        self.assertNotIn("high", followup_payload["tool_results"][0]["data"])
        self.assertEqual(followup_payload["answer_packet"]["forecast_detail"]["forecast_expected"], 112)
        self.assertEqual(
            followup_payload["answer_packet"]["forecast_detail"]["weather_context"]["condition"],
            "heavy rain",
        )

    def test_unified_chat_auto_retrieves_context_before_ai_failure_reaches_operator(self) -> None:
        self._insert_forecast()
        digest_repo = OperatorContextDigestRepository(self.db)
        digest_repo.insert_digest(
            operator_id="test_operator",
            kind="current_state",
            produced_at=datetime.now(UTC),
            payload_json=json.dumps({
                "reference_date": "2026-04-13",
                "phase": "operations",
                "headline_forecast": {"service_date": "2026-04-13", "expected": 100},
                "near_horizon": [{"service_date": "2026-04-14", "expected": 112}],
            }),
            source_hash="current",
        )
        digest_repo.insert_digest(
            operator_id="test_operator",
            kind="temporal",
            produced_at=datetime.now(UTC),
            payload_json=json.dumps({"conversation_state": "active"}),
            source_hash="temporal",
        )
        dispatcher = RecordingDispatcher(
            first_output={
                "text": "Chat could not produce a reply this time. Please try again in a moment.",
                "tool_calls": [],
                "suggested_messages": [],
            },
            second_output={
                "text": "Rain is the issue for Apr 14: the forecast has heavy rain around dinner and a 54% rain chance.",
                "tool_calls": [],
                "suggested_messages": [],
            },
        )
        service = UnifiedAgentService(self.db, provider=FakeProvider(), agent_dispatcher=dispatcher)  # type: ignore[arg-type]

        response = service.respond(
            operator_id="test_operator",
            message="how likely is the rain tomorrow?",
            reference_date=date(2026, 4, 13),
        )

        self.assertIn("54% rain chance", response.text)
        self.assertGreaterEqual(len(dispatcher.calls), 2)
        followup_payload = dict(dispatcher.calls[1].payload)
        tool_names = [result["tool"] for result in followup_payload["tool_results"]]
        self.assertEqual(tool_names, ["query_forecast_why", "query_service_weather"])
        self.assertEqual(
            followup_payload["answer_packet"]["forecast_why"]["weather_context"]["precip_chance_pct"],
            54,
        )
        self.assertEqual(
            followup_payload["answer_packet"]["service_weather"]["weather"]["precip_chance_pct"],
            54,
        )

    def test_unified_chat_preloads_why_packet_for_short_followup(self) -> None:
        self._insert_forecast()
        digest_repo = OperatorContextDigestRepository(self.db)
        digest_repo.insert_digest(
            operator_id="test_operator",
            kind="current_state",
            produced_at=datetime.now(UTC),
            payload_json=json.dumps({
                "reference_date": "2026-04-13",
                "phase": "operations",
                "headline_forecast": {"service_date": "2026-04-13", "expected": 100},
                "near_horizon": [{"service_date": "2026-04-14", "expected": 112}],
            }),
            source_hash="current",
        )
        digest_repo.insert_digest(
            operator_id="test_operator",
            kind="temporal",
            produced_at=datetime.now(UTC),
            payload_json=json.dumps({"conversation_state": "active"}),
            source_hash="temporal",
        )
        self.db.execute(
            """
            INSERT INTO conversation_messages (operator_id, role, content, phase)
            VALUES (?, ?, ?, ?)
            """,
            [
                "test_operator",
                "assistant",
                "Apr 14 dinner is sitting around 112 covers.",
                "operations",
            ],
        )
        dispatcher = RecordingDispatcher(
            first_output={
                "text": "Rain is the main reason Apr 14 is softer.",
                "tool_calls": [],
                "suggested_messages": [],
            },
            second_output={
                "text": "unused",
                "tool_calls": [],
                "suggested_messages": [],
            },
        )
        service = UnifiedAgentService(self.db, provider=FakeProvider(), agent_dispatcher=dispatcher)  # type: ignore[arg-type]

        response = service.respond(
            operator_id="test_operator",
            message="you do not know why?",
            reference_date=date(2026, 4, 13),
        )

        self.assertIn("Rain", response.text)
        first_payload = dict(dispatcher.calls[0].payload)
        self.assertEqual(first_payload["tool_results"][0]["tool"], "query_forecast_why")
        self.assertEqual(first_payload["answer_packet"]["forecast_why"]["service_date"], "2026-04-14")

    def _insert_prediction_run(self, run_id: str, *, service_date: str = "2026-04-14") -> None:
        self.db.execute(
            """
            INSERT INTO prediction_runs (
                prediction_run_id, operator_id, service_date, service_window,
                prediction_case, forecast_regime, horizon_mode, target_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                "test_operator",
                date.fromisoformat(service_date),
                ServiceWindow.DINNER.value,
                "basic_profile_no_history",
                "early_learning",
                "near_0_3",
                "total_covers",
            ],
        )

    def _insert_forecast(self) -> None:
        self.db.execute(
            """
            INSERT INTO operator_weekly_baselines (
                operator_id, service_window, day_group, baseline_total_covers, source_type
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ["test_operator", ServiceWindow.DINNER.value, "mon_thu", 100, "test"],
        )
        self._insert_prediction_run("run_forecast")
        self.db.execute(
            """
            INSERT INTO engine_digest (
                prediction_run_id, operator_id, service_date, service_window, digest_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                "run_forecast",
                "test_operator",
                date.fromisoformat("2026-04-14"),
                ServiceWindow.DINNER.value,
                json.dumps({"baseline": 100, "total_pct": 0.12, "weather_pct": -0.314}),
            ],
        )
        self.db.execute(
            """
            INSERT INTO weather_pulls (
                operator_id, source_name, retrieved_at, forecast_for_date,
                service_window, weather_feature_blob, source_freshness
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "test_operator",
                "open_meteo",
                datetime.now(UTC),
                date.fromisoformat("2026-04-14"),
                ServiceWindow.DINNER.value,
                json.dumps(
                    {
                        "conditions": "heavy rain",
                        "weather_code": 65,
                        "temperature_high": 72.1,
                        "temperature_low": 49.7,
                        "apparent_temp_7pm": 58.7,
                        "precip_prob": 0.54,
                        "precip_dinner_max": 0.512,
                        "wind_speed_mph": 10.0,
                        "cloudcover_bin": 3,
                    }
                ),
                "fresh",
            ],
        )
        self.db.execute(
            """
            INSERT INTO published_forecast_state (
                operator_id, service_date, service_window, state_version, active_service_windows,
                target_name, forecast_expected, forecast_low, forecast_high, confidence_tier,
                posture, service_state, prediction_case, forecast_regime, horizon_mode,
                top_drivers_json, major_uncertainties_json, source_prediction_run_id,
                publish_reason, publish_decision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "test_operator",
                date.fromisoformat("2026-04-14"),
                ServiceWindow.DINNER.value,
                1,
                json.dumps([ServiceWindow.DINNER.value]),
                "total_covers",
                112,
                98,
                126,
                "medium",
                "NORMAL",
                ServiceState.NORMAL.value,
                "basic_profile_no_history",
                "early_learning",
                "near_0_3",
                json.dumps(["weather_risk"]),
                json.dumps(["Rain risk"]),
                "run_forecast",
                "test",
                "publish",
            ],
        )


if __name__ == "__main__":
    unittest.main()
