import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "sync.db"),
            RuleEngine(),
        )
        self.operator = Actor("qc-op", "operator")
        self.supervisor = Actor("qc-supervisor", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _base(self):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {
                "name": "Glucose",
                "unit": "mmol/L",
                "allowed_low": 3.9,
                "allowed_high": 6.1,
                "rule_config": {"limit_sd": 3, "trend_n": 4, "consecutive_n": 4},
            },
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "qc-1"})
        instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        return assay, lot, instrument

    def _result(self, assay, lot, seq, value, run_at):
        return {
            "sequence_no": seq,
            "assay_id": assay["id"],
            "qc_lot_id": lot["id"],
            "value": value,
            "run_at": run_at,
        }

    def _qc_runs(self, instrument):
        return [
            run
            for run in self.service.list("qc_run")
            if run["data"].get("instrument_id") == instrument["id"]
        ]

    def test_session_sequence_restarts_after_power_cycle(self):
        _assay, lot, instrument = self._base()
        first = self.service.sync_instrument(
            self.operator,
            instrument["id"],
            "session-1",
            [self._result(_assay, lot, 1, 5.0, "2026-09-27T08:00:00Z")],
        )
        second = self.service.sync_instrument(
            self.operator,
            instrument["id"],
            "session-2",
            [self._result(_assay, lot, 1, 5.01, "2026-09-28T08:00:00Z")],
        )
        self.assertEqual(len(first["applied"]), 1)
        self.assertEqual(len(second["applied"]), 1)
        runs = self._qc_runs(instrument)
        self.assertEqual(len(runs), 2)
        sessions = self.service.list_sessions(instrument["id"])
        self.assertEqual({row["session_id"] for row in sessions}, {"session-1", "session-2"})

    def test_concurrent_operators_submitting_same_upload_apply_once(self):
        _assay, lot, instrument = self._base()
        results = [
            self._result(_assay, lot, 1, 5.0, "2026-09-27T08:00:00Z"),
            self._result(_assay, lot, 2, 5.01, "2026-09-27T08:01:00Z"),
            self._result(_assay, lot, 3, 5.02, "2026-09-27T08:02:00Z"),
        ]
        barrier = threading.Barrier(4)
        outcomes = []

        def worker():
            barrier.wait()
            outcomes.append(
                self.service.sync_instrument(self.operator, instrument["id"], "session-x", results)
            )

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        runs = self._qc_runs(instrument)
        self.assertEqual(len(runs), 3)
        staged = self.service.list_staged(instrument["id"], "session-x")
        self.assertEqual({row["status"] for row in staged}, {"applied"})
        total_applied = sum(len(item["applied"]) for item in outcomes)
        self.assertEqual(total_applied, 3)

    def test_backfill_by_observation_time_recomputes_and_recalls_released_batch(self):
        _assay, lot, instrument = self._base()
        # Three points already online: no trend yet (trend_n = 4).
        for value, run_at in (
            (5.00, "2026-09-27T08:00:00Z"),
            (5.01, "2026-09-27T08:01:00Z"),
            (5.02, "2026-09-27T08:02:00Z"),
        ):
            run = self.service.create(
                self.operator,
                "qc_run",
                {
                    "assay_id": _assay["id"],
                    "qc_lot_id": lot["id"],
                    "instrument_id": instrument["id"],
                    "value": value,
                    "run_at": run_at,
                },
            )
            self.service.transition(self.operator, run["id"], "evaluate", {"evaluated_by": "qc-1"})
        runs = self._qc_runs(instrument)
        latest = max(runs, key=lambda item: item["data"]["run_at"])
        self.assertEqual(latest["status"], "accepted")

        batch = self.service.create(
            self.operator,
            "result_batch",
            {
                "assay_id": _assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": latest["id"],
                "run_at": "2026-09-27T08:05:00Z",
                "patient_count": 12,
            },
        )
        batch = self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
        self.assertEqual(batch["status"], "released")

        # Offline backfill: an earlier point arrives after the batch was released.
        outcome = self.service.sync_instrument(
            self.operator,
            instrument["id"],
            "session-backfill",
            [self._result(_assay, lot, 1, 4.99, "2026-09-27T07:59:00Z")],
        )
        self.assertEqual(len(outcome["applied"]), 1)
        self.assertTrue(outcome["recalled"], "released batch must be recalled")

        refreshed = self.service.get(batch["id"])
        self.assertEqual(refreshed["status"], "intercepted")
        latest = self.service.get(latest["id"])
        self.assertEqual(latest["status"], "rejected")
        audit = self.service.audit_log()
        self.assertIn("recall", {entry["action"] for entry in audit})
        self.assertIn("recompute", {entry["action"] for entry in audit})

    def test_backfill_arrival_order_does_not_scramble_timeline(self):
        _assay, lot, instrument = self._base()
        # Submitted shuffled by sequence; the timeline is run_at order.
        results = [
            self._result(_assay, lot, 4, 5.03, "2026-09-27T08:03:00Z"),
            self._result(_assay, lot, 1, 5.00, "2026-09-27T08:00:00Z"),
            self._result(_assay, lot, 3, 5.02, "2026-09-27T08:02:00Z"),
            self._result(_assay, lot, 2, 5.01, "2026-09-27T08:01:00Z"),
        ]
        outcome = self.service.sync_instrument(
            self.operator, instrument["id"], "session-shuffled", results
        )
        self.assertEqual(len(outcome["applied"]), 4)
        runs = self._qc_runs(instrument)
        latest = max(runs, key=lambda item: item["data"]["run_at"])
        self.assertEqual(latest["status"], "rejected")
        self.assertIn("trend_up", latest["data"].get("flags", []))

    def test_retry_resumes_after_failure_without_duplicates(self):
        _assay, lot, instrument = self._base()
        good = [
            self._result(_assay, lot, 1, 5.00, "2026-09-27T08:00:00Z"),
            self._result(_assay, lot, 2, 5.01, "2026-09-27T08:01:00Z"),
            self._result(_assay, lot, 3, 5.02, "2026-09-27T08:02:00Z"),
        ]
        bad = [self._result(_assay, lot, 4, None, "2026-09-27T08:03:00Z")]
        first = self.service.sync_instrument(
            self.operator, instrument["id"], "session-resume", good + bad
        )
        self.assertEqual(len(first["applied"]), 3)
        self.assertEqual(len(first["failed"]), 1)
        staged = self.service.list_staged(instrument["id"], "session-resume")
        by_seq = {row["sequence_no"]: row["status"] for row in staged}
        self.assertEqual(by_seq[4], "failed")

        retry = self.service.sync_instrument(
            self.operator,
            instrument["id"],
            "session-resume",
            good + [self._result(_assay, lot, 4, 5.03, "2026-09-27T08:03:00Z")],
        )
        self.assertEqual(len(retry["applied"]), 1)
        self.assertEqual(len(retry["skipped"]), 3)
        runs = self._qc_runs(instrument)
        self.assertEqual(len(runs), 4)
        staged = self.service.list_staged(instrument["id"], "session-resume")
        self.assertTrue(all(row["status"] == "applied" for row in staged))


if __name__ == "__main__":
    unittest.main()
