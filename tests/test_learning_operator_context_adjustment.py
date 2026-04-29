from __future__ import annotations

from datetime import date
from pathlib import Path
import json
import tempfile
import unittest

from stormready_v3.domain.enums import ServiceWindow
from stormready_v3.learning.update import update_prediction_adaptation_for_prediction_run
from stormready_v3.storage.db import Database


class OperatorContextAdjustmentLearningTests(unittest.TestCase):
    def test_prediction_adaptation_learns_operator_context_adjustment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(db_path=Path(temp_dir) / "operator_context.duckdb")
            db.initialize()
            db.execute(
                "INSERT INTO operators (operator_id, restaurant_name) VALUES (?, ?)",
                ["op1", "Test Bistro"],
            )
            db.execute(
                """
                INSERT INTO prediction_runs (
                    prediction_run_id, operator_id, service_date, service_window,
                    prediction_case, forecast_regime, horizon_mode, target_name, generator_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "pred1",
                    "op1",
                    date(2026, 4, 14),
                    ServiceWindow.DINNER.value,
                    "basic_profile",
                    "early_learning",
                    "near_0_3",
                    "realized_total_covers",
                    "test",
                ],
            )
            db.execute(
                """
                INSERT INTO prediction_evaluations (
                    prediction_run_id, operator_id, service_date, service_window,
                    actual_total_covers, forecast_expected, forecast_low, forecast_high,
                    error_abs, error_pct, inside_interval, directional_bucket_correct,
                    service_state_learning_eligibility
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "pred1",
                    "op1",
                    date(2026, 4, 14),
                    ServiceWindow.DINNER.value,
                    126,
                    110,
                    100,
                    120,
                    16,
                    0.145,
                    False,
                    False,
                    "normal",
                ],
            )
            db.execute(
                """
                INSERT INTO engine_digest (
                    prediction_run_id, operator_id, service_date, service_window, digest_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    "pred1",
                    "op1",
                    date(2026, 4, 14),
                    ServiceWindow.DINNER.value,
                    json.dumps({"context_pct": 0.08, "weather_pct": 0.0}),
                ],
            )

            update_prediction_adaptation_for_prediction_run(
                db,
                operator_id="op1",
                prediction_run_id="pred1",
            )
            row = db.fetchone(
                """
                SELECT adaptation_key, adjustment_mid, sample_size
                FROM prediction_adaptation_state
                WHERE operator_id = ? AND adaptation_key = ?
                """,
                ["op1", "operator_context_adjustment"],
            )
            log_row = db.fetchone(
                "SELECT COUNT(*) FROM operator_context_adjustment_log WHERE operator_id = ?",
                ["op1"],
            )
            db.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], "operator_context_adjustment")
        self.assertGreater(row[1], 0)
        self.assertEqual(row[2], 1)
        self.assertEqual(log_row[0], 1)


if __name__ == "__main__":
    unittest.main()
