from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from stormready_v3.domain.enums import ServiceState, ServiceWindow
from stormready_v3.operator_text import communication_payload
from stormready_v3.storage.db import Database
from stormready_v3.workflows.retriever_hooks import _build_temporal_payload


class RetrieverHooksTemporalPayloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(db_path=Path(self._temp_dir.name) / "temporal_payload.duckdb")
        self.db.initialize()
        self.db.execute(
            "INSERT INTO operators (operator_id, restaurant_name) VALUES (?, ?)",
            ["op1", "Test Bistro"],
        )

    def tearDown(self) -> None:
        self.db.close()
        self._temp_dir.cleanup()

    def test_temporal_payload_uses_correct_dates_prompts_and_quality_gates(self) -> None:
        for offset in range(2):
            self.db.execute(
                """
                INSERT INTO operator_actuals (
                    operator_id, service_date, service_window, realized_total_covers,
                    service_state, entry_mode
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    "op1",
                    date(2026, 4, 10 + offset),
                    ServiceWindow.DINNER.value,
                    100 + offset,
                    ServiceState.NORMAL.value,
                    "test",
                ],
            )
        self.db.execute(
            """
            INSERT INTO baseline_learning_state (
                operator_id, service_window, day_group, baseline_mid,
                baseline_variability, history_depth
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ["op1", ServiceWindow.DINNER.value, "mon_thu", 110, 8, 2],
        )
        self.db.execute(
            """
            INSERT INTO component_learning_state (
                operator_id, component_name, component_state,
                semantic_clarity_score, reconciliation_quality_score,
                observation_count, history_depth_days, eligible_for_learning
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ["op1", "outside_covers", "observed", 0.4, 0.8, 1, 1, True],
        )

        conversation = SimpleNamespace(
            recent_evaluations=[
                {
                    "service_date": date(2026, 4, 11),
                    "error_pct": -0.22,
                    "service_state_learning_eligibility": "normal",
                },
                {
                    "service_date": date(2026, 4, 12),
                    "error_pct": 0.31,
                    "service_state_learning_eligibility": "excluded",
                },
            ],
            open_hypotheses=[
                {
                    "hypothesis_key": "rain_patio",
                    "status": "open",
                    "confidence": "medium",
                    "hypothesis_value": {"proposition": "Rain before dinner may cut patio covers."},
                }
            ],
            recent_learning_decisions=[{"decision_note": "Rain notes are still directional."}],
            operator_facts=[
                {
                    "fact_key": "patio::rain",
                    "fact_value": {"effect": "Heavy rain usually closes the patio."},
                    "confidence": "high",
                }
            ],
            learning_agenda=[
                {
                    "agenda_key": "confirm_rain_arrival",
                    "communication_payload": communication_payload(
                        category="open_question",
                        what_is_true_now="Rain may have arrived before dinner.",
                        one_question="Did rain arrive before 7",
                    ),
                }
            ],
            watched_external_sources=[],
            pending_corrections=[],
        )

        payload = _build_temporal_payload(db=self.db, operator_id="op1", conversation=conversation)

        self.assertEqual(payload["recent_misses_raw"][0]["service_date"], date(2026, 4, 11))
        self.assertEqual(payload["recent_misses_raw"][0]["service_state"], "normal")
        self.assertEqual(payload["recent_misses_raw"][1]["service_state"], "excluded")
        self.assertEqual(payload["open_hypotheses"][0]["proposition"], "Rain before dinner may cut patio covers.")
        self.assertEqual(payload["operator_facts_raw"][0]["value"], "Heavy rain usually closes the patio.")
        self.assertIn("Did rain arrive before 7.", payload["learning_agenda_rows"][0]["prompt"])
        self.assertEqual(payload["actual_count_total"], 2)
        self.assertEqual(payload["learning_quality"], "cold_start")
        self.assertIn("baseline", payload["cascades_live"])
        self.assertNotIn("component", payload["cascades_live"])
        self.assertEqual(payload["held_back_cascades"], ["component"])
        self.assertTrue(any("early" in warning for warning in payload["data_warnings"]))


if __name__ == "__main__":
    unittest.main()
