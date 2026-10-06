import hashlib
import json
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .repository import utcnow
from .rules import RuleEngine, evaluate_qc, timeline_key

BUFFER_ROLES = ("operator", "supervisor", "admin")
# Verdict-bearing states: only these runs participate in the timeline and
# in cascade recomputation. Pending runs await manual evaluation; the rest
# are investigation states that must not be silently overwritten.
VERDICT_STATES = ("accepted", "rejected")


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
        updated = self._apply_transition(entity, expected, next_status, patch, actor, action)
        if action == "correct" and entity["kind"] == "qc_run":
            # A corrected measurement changes the basis of everything later on
            # the same timeline; re-evaluate before the caller acts on it.
            self._apply_backfill_effects([updated["id"]])
            return self.repository.get_entity(entity_id)
        return updated

    def _apply_transition(self, entity, expected_version, next_status, patch, actor, action):
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity["id"], expected_version, next_status, merged)
        self.audit.record(
            entity["id"],
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    # ------------------------------------------------------------------
    # Onboard buffering: instruments keep QC results while offline and push
    # them after reconnecting. Onboard sequence numbers restart on every
    # power-up, so identity is (instrument, session_id, seq); each record is
    # committed with its checkpoint so a failed push resumes from the last
    # acknowledged sequence instead of inserting rows twice.
    # ------------------------------------------------------------------

    @staticmethod
    def buffer_run_id(instrument_id, session_id, seq):
        return "buffer:%s:%s:%s" % (instrument_id, session_id, int(seq))

    @staticmethod
    def _record_digest(record):
        raw = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def upload_qc_buffer(self, actor, instrument_id, session_id, records):
        if actor.role not in BUFFER_ROLES:
            raise PermissionDenied("role %s is not allowed to upload onboard QC" % actor.role)
        if not str(session_id or "").strip():
            raise ValidationError("session_id is required for onboard backhaul")
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise ValidationError("instrument does not exist")
        normalized = self._normalize_buffer_records(instrument_id, session_id, records)
        session = self.repository.start_upload_session(
            instrument_id, session_id, len(normalized), actor.user_id
        )
        if session["status"] == "completed":
            # A retry of a finished session must still prove every record
            # carries the same content; otherwise the same (session, seq)
            # would silently mean two different observations.
            for seq, record in normalized:
                stored = self.repository.get_buffer_record(instrument_id, session_id, seq)
                if stored is None:
                    raise ConflictError(
                        "session %s is completed but seq %s is missing" % (session_id, seq)
                    )
                if stored["payload_hash"] != self._record_digest(record):
                    raise ConflictError(
                        "buffer record %s#%s was already uploaded with different content"
                        % (session_id, seq)
                    )
            return self._upload_result(instrument_id, session_id, inserted=0, skipped=len(normalized), resumed=True)
        try:
            inserted = 0
            skipped = 0
            touched = []
            for seq, record in normalized:
                result = self.repository.insert_buffer_record(
                    instrument_id,
                    session_id,
                    seq,
                    self.buffer_run_id(instrument_id, session_id, seq),
                    "qc_run",
                    "pending",
                    record,
                    actor.user_id,
                    self._record_digest(record),
                )
                touched.append(result["run_entity_id"])
                if result["inserted"]:
                    inserted += 1
                    self.audit.record(
                        result["run_entity_id"],
                        actor,
                        "buffer_ingest",
                        None,
                        "pending",
                        {"instrument_id": instrument_id, "session_id": session_id, "seq": seq},
                    )
                else:
                    skipped += 1
            # Records land at their observation time, not arrival order, and a
            # single new point can flip every verdict downstream: recompute
            # the whole affected timelines and pull back released batches
            # whose basis just changed.
            self._apply_backfill_effects(touched)
            self.repository.finish_upload_session(instrument_id, session_id, "completed")
            return self._upload_result(
                instrument_id,
                session_id,
                inserted=inserted,
                skipped=skipped,
                resumed=session["last_seq"] > 0 and inserted == 0,
            )
        except Exception as exc:
            self.repository.finish_upload_session(instrument_id, session_id, "failed", str(exc))
            raise

    def _upload_result(self, instrument_id, session_id, inserted, skipped, resumed):
        session = self.repository.get_upload_session(instrument_id, session_id)
        return {
            "instrument_id": instrument_id,
            "session_id": session_id,
            "status": session["status"] if session else "unknown",
            "inserted": inserted,
            "skipped": skipped,
            "resumed": resumed,
            "checkpoint": session["last_seq"] if session else 0,
            "total": session["total"] if session else 0,
        }

    def _normalize_buffer_records(self, instrument_id, session_id, records):
        if not isinstance(records, list) or not records:
            raise ValidationError("records must be a non-empty list")
        ordered = []
        seen = set()
        for item in records:
            if not isinstance(item, dict):
                raise ValidationError("each buffered record must be an object")
            seq = item.get("seq")
            if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
                raise ValidationError("onboard seq must be a non-negative integer")
            if seq in seen:
                raise ValidationError("duplicate seq within upload: %s" % seq)
            seen.add(seq)
            record = {
                "assay_id": item.get("assay_id"),
                "qc_lot_id": item.get("qc_lot_id"),
                "instrument_id": instrument_id,
                "value": item.get("value"),
                "run_at": item.get("run_at"),
                "source": "onboard_buffer",
                "session_id": session_id,
                "seq": seq,
            }
            # Reuse the standard qc_run validation (assay/lot/value checks)
            # and take its normalization (e.g. value coerced to float).
            normalized_fields = self.rules.validate_create(
                _BufferActor(), "qc_run", dict(record), self._lookup
            )
            record.update(normalized_fields)
            ordered.append((seq, record))
        ordered.sort(key=lambda item: item[0])
        return ordered

    def _apply_backfill_effects(self, run_ids):
        """Recompute QC timelines after late data, then pull back batches.

        Newly ingested buffered runs are still 'pending'; cascade takes them
        straight to a verdict. Previously evaluated runs (accepted/rejected)
        are recomputed in observation-time order and overwritten whenever the
        flags or verdict change. Released patient result batches whose basis
        run flips accepted -> rejected are returned to 'intercepted'.
        """
        affected_runs = [self.repository.get_entity(run_id) for run_id in run_ids]
        affected_runs = [run for run in affected_runs if run and run["kind"] == "qc_run"]
        affected_ids = {run["id"] for run in affected_runs}
        instruments = {run["data"]["instrument_id"] for run in affected_runs}
        lots = {run["data"]["qc_lot_id"] for run in affected_runs}

        changed_basis = []
        for instrument_id in instruments:
            runs = [
                run
                for run in self.repository.list_entities(kind="qc_run")
                if run["data"].get("instrument_id") == instrument_id
            ]
            by_lot = {}
            for run in runs:
                eligible = run["status"] in VERDICT_STATES or (
                    run["id"] in affected_ids and run["status"] == "pending"
                )
                if not eligible:
                    # Unaffected pending runs await manual evaluation;
                    # investigation states must not be silently overwritten.
                    continue
                by_lot.setdefault(run["data"].get("qc_lot_id"), []).append(run)
            for lot_id, timeline in by_lot.items():
                if lot_id not in lots:
                    continue
                timeline.sort(key=timeline_key)
                lot = self.repository.get_entity(lot_id)
                if not lot:
                    continue
                assay = self.repository.get_entity(lot["data"].get("assay_id"))
                if not assay:
                    continue
                history = []
                for run in timeline:
                    verdict = evaluate_qc(
                        history,
                        run["data"].get("value"),
                        lot["data"].get("target"),
                        lot["data"].get("sd"),
                        assay["data"].get("rule_config"),
                    )
                    fresh = self._store_recomputed_verdict(run, verdict)
                    if fresh is not None:
                        changed_basis.append(fresh)
                    history.append(float(run["data"].get("value")))
        pulled = self._pull_back_released_batches(changed_basis)
        return {"changed_runs": changed_basis, "pulled_batches": pulled}

    def _store_recomputed_verdict(self, run, verdict):
        """Persist a cascade verdict; return (run, old_status) when it flipped."""
        new_status = "accepted" if verdict["accepted"] else "rejected"
        flags = verdict.get("flags", [])
        same_verdict = run["status"] == new_status
        same_flags = list(run["data"].get("flags") or []) == flags
        same_z = run["data"].get("z_score") == verdict["z_score"]
        if same_verdict and same_flags and same_z:
            return None
        old_status = run["status"]
        merged = dict(run["data"])
        merged["flags"] = flags
        merged["z_score"] = verdict["z_score"]
        merged["rule_snapshot"] = verdict.get("rule_snapshot")
        merged["verdict_at"] = utcnow()
        if new_status == "rejected":
            merged.setdefault("reject_reason", "quality control rule violation")
        else:
            merged["reject_reason"] = None
        actor = _SystemActor()
        # expected_version=None: two operators uploading the same session may
        # race through the same recomputation; the result is deterministic for
        # a fixed timeline, so the losing writer simply overwrites with the
        # same verdict instead of surfacing a version conflict.
        updated = self.repository.update_entity(run["id"], None, new_status, merged)
        self.audit.record(
            run["id"],
            actor,
            "cascade_evaluate",
            old_status,
            new_status,
            {"flags": flags, "z_score": verdict["z_score"]},
        )
        return (updated, old_status)

    def _pull_back_released_batches(self, changed_basis):
        """Intercept released batches whose basis run turned rejected."""
        pulled = []
        flipped_ids = {
            run["id"]
            for run, old_status in changed_basis
            if old_status == "accepted" and run["status"] == "rejected"
        }
        if not flipped_ids:
            return pulled
        for batch in self.repository.list_entities(kind="result_batch"):
            if batch["status"] != "released":
                continue
            if batch["data"].get("qc_run_id") not in flipped_ids:
                continue
            run = self.repository.get_entity(batch["data"]["qc_run_id"])
            patch = {
                "reason": "QC basis flipped to rejected after late onboard results",
                "pullback": {
                    "qc_run_id": run["id"],
                    "flags": run["data"].get("flags"),
                    "pulled_at": utcnow(),
                },
            }
            try:
                updated = self._apply_transition(batch, batch["version"], "intercepted", patch, _SystemActor(), "intercept")
                pulled.append(updated["id"])
            except ConflictError:
                # Version raced with a manual action; reload and retry once.
                reloaded = self.repository.get_entity(batch["id"])
                if reloaded and reloaded["status"] == "released":
                    updated = self._apply_transition(
                        reloaded, reloaded["version"], "intercepted", patch, _SystemActor(), "intercept"
                    )
                    pulled.append(updated["id"])
        return pulled

    def get_upload_session(self, instrument_id, session_id):
        session = self.repository.get_upload_session(instrument_id, session_id)
        if not session:
            raise NotFoundError("upload session not found: %s/%s" % (instrument_id, session_id))
        return session

    def list_upload_sessions(self, instrument_id=None):
        return self.repository.list_upload_sessions(instrument_id)

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)


class _BufferActor:
    """Validation-only actor for buffered records; role is allowed for qc_run."""

    user_id = "onboard-buffer"
    role = "operator"


class _SystemActor:
    """Actor attributed to automatic cascade actions triggered by new evidence."""

    user_id = "system:cascade"
    role = "operator"
