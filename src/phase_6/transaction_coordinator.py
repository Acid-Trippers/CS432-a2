import json
import os
import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional


class TransactionStep:
    def __init__(
        self,
        name: str,
        participant: str,
        apply_fn: Callable[[], Any],
        compensate_fn: Callable[[], Any],
        verify_fn: Optional[Callable[[Any], bool]] = None,
    ):
        self.name = name
        self.participant = participant
        self.apply_fn = apply_fn
        self.compensate_fn = compensate_fn
        self.verify_fn = verify_fn


class TransactionCoordinator:
    """
    Saga-style coordinator with compensation and a JSON transaction log.
    """

    _log_lock = threading.Lock()

    def __init__(self, log_file: str):
        self.log_file = log_file
        self._ensure_log_file()

    def _ensure_log_file(self):
        log_dir = os.path.dirname(self.log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        if not os.path.exists(self.log_file):
            self._atomic_write_json(self.log_file, [])

    @staticmethod
    def _atomic_write_json(file_path: str, data: Any):
        temp_path = f"{file_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)
        os.replace(temp_path, file_path)

    def _append_log(self, transaction: Dict[str, Any]):
        with self._log_lock:
            try:
                with open(self.log_file, "r", encoding="utf-8") as fh:
                    content = fh.read().strip()
                    log_data = json.loads(content) if content else []
            except (json.JSONDecodeError, FileNotFoundError):
                log_data = []

            if not isinstance(log_data, list):
                log_data = []

            log_data.append(transaction)
            self._atomic_write_json(self.log_file, log_data)

    def run(
        self,
        operation: str,
        entity: str,
        participants: List[str],
        steps: List[TransactionStep],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        tx = {
            "transaction_id": str(uuid.uuid4()),
            "operation": operation,
            "entity": entity,
            "participants": participants,
            "state": "in_progress",
            "started_at": datetime.utcnow().isoformat() + "Z",
            "ended_at": None,
            "events": [],
            "metadata": metadata or {},
        }

        applied_steps: List[TransactionStep] = []

        def record(event_type: str, payload: Optional[Dict[str, Any]] = None):
            tx["events"].append(
                {
                    "time": datetime.utcnow().isoformat() + "Z",
                    "type": event_type,
                    "payload": payload or {},
                }
            )

        record("transaction_started")

        try:
            for step in steps:
                record(
                    "step_apply_started",
                    {"step": step.name, "participant": step.participant},
                )
                result = step.apply_fn()

                if step.verify_fn is not None and not step.verify_fn(result):
                    raise RuntimeError(
                        f"Verification failed for step '{step.name}' ({step.participant})"
                    )

                applied_steps.append(step)
                record(
                    "step_apply_succeeded",
                    {"step": step.name, "participant": step.participant},
                )

            tx["state"] = "committed"
            tx["ended_at"] = datetime.utcnow().isoformat() + "Z"
            record("transaction_committed")
            self._append_log(tx)
            return {
                "success": True,
                "transaction_id": tx["transaction_id"],
                "state": tx["state"],
                "events": tx["events"],
            }

        except Exception as apply_error:
            record(
                "transaction_apply_failed",
                {
                    "error": str(apply_error),
                    "applied_steps": [s.name for s in applied_steps],
                },
            )

            compensation_errors: List[Dict[str, str]] = []
            for step in reversed(applied_steps):
                try:
                    record(
                        "step_compensate_started",
                        {"step": step.name, "participant": step.participant},
                    )
                    step.compensate_fn()
                    record(
                        "step_compensate_succeeded",
                        {"step": step.name, "participant": step.participant},
                    )
                except Exception as compensation_error:
                    compensation_errors.append(
                        {
                            "step": step.name,
                            "participant": step.participant,
                            "error": str(compensation_error),
                        }
                    )
                    record(
                        "step_compensate_failed",
                        {
                            "step": step.name,
                            "participant": step.participant,
                            "error": str(compensation_error),
                        },
                    )

            if compensation_errors:
                tx["state"] = "failed_needs_recovery"
                record(
                    "transaction_rollback_incomplete",
                    {"compensation_errors": compensation_errors},
                )
            else:
                tx["state"] = "rolled_back"
                record("transaction_rolled_back")

            tx["ended_at"] = datetime.utcnow().isoformat() + "Z"
            self._append_log(tx)

            return {
                "success": False,
                "transaction_id": tx["transaction_id"],
                "state": tx["state"],
                "error": str(apply_error),
                "compensation_errors": compensation_errors,
                "events": tx["events"],
            }
