import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from src.domain import Actor, ConflictError
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class BufferTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "buffer.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.operator = Actor("op-1", "operator")

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

    def _records(self, lot, instrument, pairs):
        return [
            {
                "assay_id": lot["data"]["assay_id"],
                "qc_lot_id": lot["id"],
                "seq": seq,
                "value": value,
                "run_at": run_at,
            }
            for seq, (run_at, value) in enumerate(pairs)
        ]

    def _runs_by_time(self, instrument):
        runs = self.service.list("qc_runs")
        return sorted(runs, key=lambda run: run["data"]["run_at"])

    # ------------------------------------------------------------------

    def test_scrambled_arrival_is_inserted_on_observation_timeline(self):
        assay, lot, instrument = self._base()
        records = self._records(
            lot,
            instrument,
            [
                ("2026-10-01T08:00:00Z", 5.00),
                ("2026-10-01T09:00:00Z", 5.03),
                ("2026-10-01T10:00:00Z", 5.06),
                ("2026-10-01T11:00:00Z", 5.09),
            ],
        )
        # The instrument replays its backlog out of arrival order.
        scrambled = [records[i] for i in (3, 0, 2, 1)]
        result = self.service.upload_qc_buffer(
            self.operator, instrument["id"], "boot-1", scrambled
        )
        self.assertEqual(result["inserted"], 4)
        runs = self._runs_by_time(instrument)
        self.assertEqual([run["data"]["value"] for run in runs], [5.00, 5.03, 5.06, 5.09])
        # Ordered by observation time the window is strictly increasing;
        # ordered by arrival it would not be a trend at all.
        self.assertEqual(runs[0]["status"], "accepted")
        self.assertEqual(runs[-1]["status"], "rejected")
        self.assertIn("trend_up", runs[-1]["data"]["flags"])
        for run in runs:
            self.assertEqual(run["data"]["source"], "onboard_buffer")
            self.assertEqual(run["data"]["session_id"], "boot-1")

    def test_late_backfill_recomputes_and_pulls_released_batch_back(self):
        assay, lot, instrument = self._base()
        # First power session reaches the host on time. Three high points do
        # not yet make a run of four, so the last run is accepted and a
        # patient batch is released against it.
        first = self.service.upload_qc_buffer(
            self.operator,
            instrument["id"],
            "boot-1",
            self._records(
                lot,
                instrument,
                [
                    ("2026-10-02T09:00:00Z", 5.13),
                    ("2026-10-02T10:00:00Z", 5.14),
                    ("2026-10-02T11:00:00Z", 5.15),
                ],
            ),
        )
        self.assertEqual(first["inserted"], 3)
        runs = self._runs_by_time(instrument)
        self.assertTrue(all(run["status"] == "accepted" for run in runs))
        basis = runs[-1]
        batch = self.service.create(
            self.supervisor,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": basis["id"],
                "run_at": "2026-10-02T11:05:00Z",
                "patient_count": 20,
            },
        )
        batch = self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
        self.assertEqual(batch["status"], "released")

        # New power session: onboard seq restarts from 0. It carries a point
        # observed earlier than everything in the host. Four consecutive high
        # points now form a bias, so the released basis flips to rejected.
        second = self.service.upload_qc_buffer(
            self.operator,
            instrument["id"],
            "boot-2",
            [
                {
                    "assay_id": lot["data"]["assay_id"],
                    "qc_lot_id": lot["id"],
                    "seq": 0,
                    "value": 5.12,
                    "run_at": "2026-10-02T08:00:00Z",
                }
            ],
        )
        self.assertEqual(second["inserted"], 1)
        runs = self._runs_by_time(instrument)
        self.assertEqual(runs[0]["data"]["session_id"], "boot-2")
        self.assertEqual(runs[0]["data"]["seq"], 0)
        flipped = next(run for run in runs if run["id"] == basis["id"])
        self.assertEqual(flipped["status"], "rejected")
        self.assertIn("bias_high", flipped["data"]["flags"])
        batch = self.service.get(batch["id"])
        self.assertEqual(batch["status"], "intercepted")
        self.assertEqual(batch["data"]["pullback"]["qc_run_id"], basis["id"])
        audit = self.service.audit_log(batch["id"])
        self.assertEqual(audit[-1]["action"], "intercept")
        self.assertEqual(audit[-1]["actor_id"], "system:cascade")
        self.assertEqual(audit[-1]["from_status"], "released")

    def test_concurrent_submissions_insert_once(self):
        assay, lot, instrument = self._base()
        records = self._records(
            lot,
            instrument,
            [
                ("2026-10-03T08:00:00Z", 5.00),
                ("2026-10-03T09:00:00Z", 5.01),
                ("2026-10-03T10:00:00Z", 5.02),
            ],
        )
        results = []
        errors = []

        def submit(actor):
            try:
                results.append(
                    self.service.upload_qc_buffer(
                        actor, instrument["id"], "boot-9", records
                    )
                )
            except Exception as exc:  # pragma: no cover - surfaced via assertion
                errors.append(exc)

        t1 = threading.Thread(target=submit, args=(Actor("op-a", "operator"),))
        t2 = threading.Thread(target=submit, args=(Actor("op-b", "operator"),))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(sum(item["inserted"] for item in results), 3)
        self.assertEqual(sum(item["skipped"] for item in results), 3)
        runs = [
            run
            for run in self.service.list("qc_runs")
            if run["data"].get("session_id") == "boot-9"
        ]
        self.assertEqual(len(runs), 3)
        session = self.service.get_upload_session(instrument["id"], "boot-9")
        self.assertEqual(session["status"], "completed")
        self.assertEqual(session["last_seq"], 2)

    def test_failed_upload_resumes_from_last_checkpoint(self):
        assay, lot, instrument = self._base()
        records = self._records(
            lot,
            instrument,
            [
                ("2026-10-04T08:00:00Z", 5.00),
                ("2026-10-04T09:00:00Z", 5.01),
                ("2026-10-04T10:00:00Z", 5.02),
                ("2026-10-04T11:00:00Z", 5.03),
            ],
        )
        repository = self.service.repository
        original = repository.insert_buffer_record
        state = {"calls": 0}

        def flaky(*args, **kwargs):
            state["calls"] += 1
            if state["calls"] == 3:
                raise RuntimeError("simulated storage outage")
            return original(*args, **kwargs)

        repository.insert_buffer_record = flaky
        with self.assertRaises(RuntimeError):
            self.service.upload_qc_buffer(self.operator, instrument["id"], "boot-x", records)
        repository.insert_buffer_record = original

        session = self.service.get_upload_session(instrument["id"], "boot-x")
        self.assertEqual(session["status"], "failed")
        self.assertEqual(session["last_seq"], 1)
        runs = [
            run
            for run in self.service.list("qc_runs")
            if run["data"].get("session_id") == "boot-x"
        ]
        self.assertEqual(len(runs), 2)

        # Retry carries the same payload; already-stored seqs are skipped and
        # the remaining records continue from the checkpoint.
        retry = self.service.upload_qc_buffer(self.operator, instrument["id"], "boot-x", records)
        self.assertEqual(retry["inserted"], 2)
        self.assertEqual(retry["skipped"], 2)
        self.assertEqual(retry["status"], "completed")
        runs = [
            run
            for run in self.service.list("qc_runs")
            if run["data"].get("session_id") == "boot-x"
        ]
        self.assertEqual(len(runs), 4)

    def test_same_seq_in_different_sessions_are_distinct_runs(self):
        assay, lot, instrument = self._base()
        for session, run_at in (("boot-1", "2026-10-05T08:00:00Z"), ("boot-2", "2026-10-06T08:00:00Z")):
            result = self.service.upload_qc_buffer(
                self.operator,
                instrument["id"],
                session,
                [
                    {
                        "assay_id": lot["data"]["assay_id"],
                        "qc_lot_id": lot["id"],
                        "seq": 0,
                        "value": 5.0,
                        "run_at": run_at,
                    }
                ],
            )
            self.assertEqual(result["inserted"], 1)
        self.assertEqual(len(self.service.list("qc_runs")), 2)

    def test_same_seq_with_different_content_is_conflict(self):
        assay, lot, instrument = self._base()
        payload = [
            {
                "assay_id": lot["data"]["assay_id"],
                "qc_lot_id": lot["id"],
                "seq": 0,
                "value": 5.0,
                "run_at": "2026-10-07T08:00:00Z",
            }
        ]
        self.service.upload_qc_buffer(self.operator, instrument["id"], "boot-z", payload)
        tampered = [dict(payload[0], value=5.9)]
        with self.assertRaises(ConflictError):
            self.service.upload_qc_buffer(self.operator, instrument["id"], "boot-z", tampered)

    def test_correction_recomputes_timeline(self):
        assay, lot, instrument = self._base()
        self.service.upload_qc_buffer(
            self.operator,
            instrument["id"],
            "boot-1",
            self._records(lot, instrument, [("2026-10-08T08:00:00Z", 5.00)]),
        )
        run = self._runs_by_time(instrument)[0]
        self.assertEqual(run["status"], "accepted")
        run = self.service.transition(
            self.supervisor,
            run["id"],
            "correct",
            {"reason": "wrong dilution factor", "value": 5.5},
        )
        self.assertEqual(run["status"], "rejected")
        self.assertIn("1_3s", run["data"]["flags"])

    def test_unrelated_pending_manual_run_is_not_touched(self):
        assay, lot, instrument = self._base()
        pending = self.service.create(
            self.operator,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": 5.0,
                "run_at": "2026-10-09T07:00:00Z",
            },
        )
        self.service.upload_qc_buffer(
            self.operator,
            instrument["id"],
            "boot-1",
            self._records(lot, instrument, [("2026-10-09T08:00:00Z", 5.01)]),
        )
        self.assertEqual(self.service.get(pending["id"])["status"], "pending")


class BufferHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "http.db"),
            RuleEngine(),
        )
        self.server = create_server("127.0.0.1", 0, self.service, RuleEngine(), str(Path(__file__).resolve().parent.parent / "static"))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.supervisor = Actor("qc-supervisor", "supervisor")
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "qc-1"})
        self.instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        self.lot = lot
        self.base = "http://127.0.0.1:%s" % self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def _request(self, method, path, payload=None, role="operator"):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(self.base + path, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        request.add_header("X-User-Id", "op-http")
        request.add_header("X-Role", role)
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read().decode("utf-8"))

    def test_upload_and_session_status_endpoints(self):
        body = {
            "session_id": "boot-http",
            "records": [
                {
                    "assay_id": self.lot["data"]["assay_id"],
                    "qc_lot_id": self.lot["id"],
                    "seq": 0,
                    "value": 5.0,
                    "run_at": "2026-10-10T08:00:00Z",
                }
            ],
        }
        status, result = self._request(
            "POST", "/api/instruments/%s/qc-buffer-uploads" % self.instrument["id"], body
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["status"], "completed")

        status, session = self._request(
            "GET", "/api/instruments/%s/qc-buffer-uploads/boot-http" % self.instrument["id"]
        )
        self.assertEqual(status, 200)
        self.assertEqual(session["last_seq"], 0)

        status, listing = self._request(
            "GET", "/api/instruments/%s/qc-buffer-uploads" % self.instrument["id"]
        )
        self.assertEqual(len(listing["items"]), 1)

        # Idempotent retry over HTTP returns the same completed session.
        status, retry = self._request(
            "POST", "/api/instruments/%s/qc-buffer-uploads" % self.instrument["id"], body
        )
        self.assertEqual(retry["inserted"], 0)
        self.assertEqual(retry["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
