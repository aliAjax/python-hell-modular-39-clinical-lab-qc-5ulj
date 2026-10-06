from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, ValidationError
from .repository import utcnow
from .rules import RuleEngine, evaluate_qc


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def list_sessions(self, instrument_id):
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise NotFoundError("instrument not found: " + instrument_id)
        return self.repository.list_sessions(instrument_id)

    def list_staged(self, instrument_id, session_id):
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise NotFoundError("instrument not found: " + instrument_id)
        return self.repository.list_staged(instrument_id, session_id)

    def sync_instrument(self, actor, instrument_id, session_id, results):
        """Upload offline QC results from an instrument.

        The instrument's internal sequence numbers restart after every power
        cycle, so ``session_id`` (the current power session) disambiguates
        them. Each (instrument, session, sequence) record is applied exactly
        once even when two operators submit the same upload concurrently;
        failed records are persisted and a retry resumes from them.
        """
        self.rules._ensure_role(actor, ("operator", "supervisor", "admin"))
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise NotFoundError("instrument not found: " + instrument_id)
        session_id = str(session_id or "").strip()
        if not session_id:
            raise ValidationError("session_id is required")
        if not isinstance(results, list) or not results:
            raise ValidationError("results must be a non-empty list")
        self.repository.upsert_session(instrument_id, session_id, actor.user_id)

        applied, failed, skipped = [], [], []
        for item in sorted(results, key=lambda record: record.get("sequence_no", 0)):
            try:
                sequence_no = int(item.get("sequence_no"))
            except (TypeError, ValueError):
                raise ValidationError("sequence_no must be an integer")
            payload = {
                "assay_id": item.get("assay_id"),
                "qc_lot_id": item.get("qc_lot_id"),
                "instrument_id": instrument_id,
                "value": item.get("value"),
                "run_at": item.get("run_at"),
                "session_id": session_id,
                "sequence_no": sequence_no,
            }
            try:
                validated = self.rules.validate_create(actor, "qc_run", payload, self._lookup)
                if validated:
                    payload.update(validated)
                entity_id = str(uuid4())
                status = self.rules.initial_status("qc_run")
                outcome, created_id = self.repository.apply_staged_run(
                    instrument_id,
                    session_id,
                    sequence_no,
                    item,
                    entity_id,
                    status,
                    payload,
                    actor.user_id,
                )
            except Exception as exc:
                self.repository.mark_staged_failed(
                    instrument_id, session_id, sequence_no, item, str(exc)
                )
                failed.append({"sequence_no": sequence_no, "error": str(exc)})
                continue
            if outcome == "exists":
                skipped.append(sequence_no)
                continue
            self.audit.record(
                entity_id,
                actor,
                "create",
                None,
                status,
                {
                    "kind": "qc_run",
                    "sync": True,
                    "session_id": session_id,
                    "sequence_no": sequence_no,
                },
            )
            applied.append({"sequence_no": sequence_no, "qc_run_id": entity_id})

        sweep = self.recompute_instrument(actor, instrument_id)
        return {
            "session_id": session_id,
            "applied": applied,
            "failed": failed,
            "skipped": skipped,
            "recomputed": sweep["recomputed"],
            "recalled": sweep["recalled"],
        }

    def recompute_instrument(self, actor, instrument_id):
        """Replay the instrument's QC timeline in observation order.

        Offline records arrive by arrival order, but consecutive-bias and
        trend rules only make sense over ``run_at`` order, so every run that
        has not been manually closed is re-evaluated against the full
        timeline. Runs whose verdict flips are re-audited, and released
        result batches whose QC basis is now rejected are recalled to
        interception.
        """
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise NotFoundError("instrument not found: " + instrument_id)

        runs = [
            entity
            for entity in self.repository.list_entities(kind="qc_run")
            if entity["data"].get("instrument_id") == instrument_id
        ]

        def timeline_key(run):
            data = run["data"]
            return (
                str(data.get("run_at", "")),
                str(data.get("session_id", "")),
                int(data.get("sequence_no") or 0),
                run["created_at"],
                run["id"],
            )

        runs.sort(key=timeline_key)
        by_lot = {}
        for run in runs:
            by_lot.setdefault(run["data"].get("qc_lot_id"), []).append(run)

        recomputed = []
        for lot_id, lot_runs in by_lot.items():
            lot = self.repository.get_entity(lot_id)
            if not lot:
                continue
            assay = self.repository.get_entity(lot["data"].get("assay_id"))
            if not assay:
                continue
            history = []
            for run in lot_runs:
                data = run["data"]
                if run["status"] in ("pending", "accepted", "rejected"):
                    result = evaluate_qc(
                        history,
                        data.get("value"),
                        lot["data"].get("target"),
                        lot["data"].get("sd"),
                        assay["data"].get("rule_config"),
                    )
                    next_status = "accepted" if result["accepted"] else "rejected"
                    if run["status"] != next_status:
                        refreshed_data = {
                            **data,
                            "flags": result["flags"],
                            "z_score": result["z_score"],
                            "rule_snapshot": result["rule_snapshot"],
                        }
                        self.repository.update_entity(
                            run["id"], run["version"], next_status, refreshed_data
                        )
                        self.audit.record(
                            run["id"],
                            actor,
                            "recompute",
                            run["status"],
                            next_status,
                            {"flags": result["flags"], "trigger": "timeline_recompute"},
                        )
                        recomputed.append(
                            {
                                "qc_run_id": run["id"],
                                "from": run["status"],
                                "to": next_status,
                                "flags": result["flags"],
                            }
                        )
                history.append(float(data.get("value")))

        recalled = []
        for batch in self.repository.list_entities(kind="result_batch"):
            if batch["data"].get("instrument_id") != instrument_id:
                continue
            if batch["status"] != "released":
                continue
            run = self.repository.get_entity(batch["data"].get("qc_run_id"))
            if run and run["status"] == "rejected":
                self.repository.update_entity(
                    batch["id"],
                    batch["version"],
                    "intercepted",
                    {
                        **batch["data"],
                        "recall_reason": "qc_run_rejected_after_timeline_recompute",
                        "recalled_at": utcnow(),
                    },
                )
                self.audit.record(
                    batch["id"],
                    actor,
                    "recall",
                    "released",
                    "intercepted",
                    {
                        "qc_run_id": run["id"],
                        "reason": "basis qc run rejected after timeline recompute",
                    },
                )
                recalled.append(batch["id"])
        return {"recomputed": recomputed, "recalled": recalled}

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
