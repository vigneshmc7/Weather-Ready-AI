from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
import tempfile
import unittest

from stormready_v3.domain.enums import ServiceWindow
from stormready_v3.domain.models import OperatorProfile
from stormready_v3.orchestration.orchestrator import DeterministicOrchestrator
from stormready_v3.sources.contracts import SourcePayload
from stormready_v3.storage.db import Database
from stormready_v3.storage.repositories import OperatorRepository


class _SourceRegistry:
    def list_sources(self) -> list[str]:
        return ["ok_source", "bad_source"]

    def fetch(self, source_name: str, **kwargs) -> SourcePayload:  # noqa: ANN003
        if source_name == "bad_source":
            raise RuntimeError("source failed")
        return SourcePayload(
            source_name=source_name,
            source_class="test_source",
            retrieved_at=kwargs["at"],
            payload={"findings": [{"id": "one"}]},
            freshness="fresh",
            service_date=kwargs["service_date"],
            service_window=kwargs["service_window"].value,
            source_bucket="curated_local",
            provenance={"check_mode": "live"},
        )


class SourceCheckLoggingTests(unittest.TestCase):
    def test_fetch_source_payloads_records_success_and_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(db_path=Path(temp_dir) / "source_checks.duckdb")
            db.initialize()
            profile = OperatorProfile(operator_id="op1", restaurant_name="Test Bistro")
            OperatorRepository(db).upsert_operator(profile)
            orchestrator = DeterministicOrchestrator.__new__(DeterministicOrchestrator)
            orchestrator.db = db
            orchestrator.source_registry = _SourceRegistry()

            payloads, failures = orchestrator.fetch_source_payloads(
                profile=profile,
                service_date=date(2026, 4, 14),
                service_window=ServiceWindow.DINNER,
                fetched_at=datetime(2026, 4, 14, 12, 0, tzinfo=UTC),
                refresh_run_id="refresh_1",
            )
            rows = db.fetchall(
                """
                SELECT source_name, status, findings_count, used_count, failure_reason
                FROM source_check_log
                WHERE operator_id = ?
                ORDER BY source_name
                """,
                ["op1"],
            )
            db.close()

        self.assertEqual([payload.source_name for payload in payloads], ["ok_source"])
        self.assertEqual(failures[0]["source_name"], "bad_source")
        self.assertEqual(rows[0][0], "bad_source")
        self.assertEqual(rows[0][1], "failed")
        self.assertIn("RuntimeError", rows[0][4])
        self.assertEqual(rows[1][0], "ok_source")
        self.assertEqual(rows[1][1], "fresh")
        self.assertEqual(rows[1][2], 1)


if __name__ == "__main__":
    unittest.main()
