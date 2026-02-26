import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class RunContext:
    run_id: str
    root_dir: Path

    @property
    def run_dir(self) -> Path:
        return self.root_dir / self.run_id

    @property
    def batches_dir(self) -> Path:
        return self.run_dir / "batches"

    @property
    def header_dir(self) -> Path:
        return self.run_dir / "header"


class RunLogs:
    def __init__(self, root_dir: Path, run_id: Optional[str] = None):
        self.ctx = RunContext(
            run_id=run_id or datetime.now().strftime("run_%Y%m%d_%H%M%S"),
            root_dir=root_dir,
        )
        self.ctx.run_dir.mkdir(parents=True, exist_ok=True)
        self.ctx.batches_dir.mkdir(parents=True, exist_ok=True)
        self.ctx.header_dir.mkdir(parents=True, exist_ok=True)
        self.index: Dict[str, Any] = {
            "run_id": self.ctx.run_id,
            "created_at": datetime.now().isoformat(),
            "header_attempts": [],
            "batches": {},
        }

    def write_text(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", errors="ignore")

    def write_json(self, path: Path, data: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def log_source_text(self, header_text: str, operations_text: str) -> None:
        self.write_text(self.ctx.run_dir / "source_header.txt", header_text)
        self.write_text(self.ctx.run_dir / "source_operations.txt", operations_text)
        self.flush_index()

    def log_header_attempt(
        self,
        attempt: int,
        prompt: str,
        response: str,
        error: Optional[str],
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        base = self.ctx.header_dir / f"attempt_{attempt:02d}"
        self.write_text(base / "prompt.txt", prompt)
        self.write_text(base / "response.txt", response)
        if error:
            self.write_text(base / "error.txt", error)
        payload = {
            "attempt": attempt,
            "meta": meta or {},
            "prompt": prompt,
            "response": response,
            "error": error,
        }
        self.write_json(base / "event.json", payload)
        self.index["header_attempts"].append(
            {
                "attempt": attempt,
                "path": str((Path("header") / f"attempt_{attempt:02d}").as_posix()),
                "error": error,
            }
        )
        self.flush_index()

    def log_batch_input(self, batch_number: int, batch_text: str) -> None:
        base = self.ctx.batches_dir / f"batch_{batch_number:04d}"
        self.write_text(base / "input.txt", batch_text)
        self.index["batches"].setdefault(str(batch_number), {"attempts": []})
        self.index["batches"][str(batch_number)]["input_path"] = str(
            (Path("batches") / f"batch_{batch_number:04d}" / "input.txt").as_posix()
        )
        self.flush_index()

    def log_batch_attempt(
        self,
        batch_number: int,
        attempt: int,
        prompt: str,
        response: str,
        error: Optional[str],
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        base = self.ctx.batches_dir / f"batch_{batch_number:04d}" / f"attempt_{attempt:02d}"
        self.write_text(base / "prompt.txt", prompt)
        self.write_text(base / "response.txt", response)
        if error:
            self.write_text(base / "error.txt", error)
        payload = {
            "batch_number": batch_number,
            "attempt": attempt,
            "meta": meta or {},
            "prompt": prompt,
            "response": response,
            "error": error,
        }
        self.write_json(base / "event.json", payload)
        self.index["batches"].setdefault(str(batch_number), {"attempts": []})
        self.index["batches"][str(batch_number)]["attempts"].append(
            {
                "attempt": attempt,
                "path": str((Path("batches") / f"batch_{batch_number:04d}" / f"attempt_{attempt:02d}").as_posix()),
                "error": error,
            }
        )
        self.flush_index()

    def log_batch_recovery(self, batch_number: int, operations: list[dict[str, Any]], reason: str) -> None:
        base = self.ctx.batches_dir / f"batch_{batch_number:04d}"
        debit_sum, credit_sum = compute_totals(operations)
        self.write_json(
            base / "recovery.json",
            {
                "batch_number": batch_number,
                "reason": reason,
                "operations_count": len(operations),
                "debit_sum": debit_sum,
                "credit_sum": credit_sum,
                "operations": operations,
            },
        )
        self.index["batches"].setdefault(str(batch_number), {"attempts": []})
        self.index["batches"][str(batch_number)]["recovery_path"] = str(
            (Path("batches") / f"batch_{batch_number:04d}" / "recovery.json").as_posix()
        )
        self.flush_index()

    def log_batch_selection(self, batch_number: int, meta: Dict[str, Any]) -> None:
        base = self.ctx.batches_dir / f"batch_{batch_number:04d}"
        self.write_json(
            base / "selection.json",
            {
                "batch_number": batch_number,
                "meta": meta,
            },
        )
        self.index["batches"].setdefault(str(batch_number), {"attempts": []})
        self.index["batches"][str(batch_number)]["selection_path"] = str(
            (Path("batches") / f"batch_{batch_number:04d}" / "selection.json").as_posix()
        )
        self.flush_index()

    def log_summary(self, data: Dict[str, Any]) -> None:
        self.write_json(self.ctx.run_dir / "summary.json", data)
        self.index["summary_path"] = "summary.json"
        self.flush_index()

    def flush_index(self) -> None:
        self.write_json(self.ctx.run_dir / "index.json", self.index)


def compute_totals(operations: list[dict[str, Any]]) -> tuple[float, float]:
    debit_sum = 0.0
    credit_sum = 0.0
    for op in operations:
        debit_sum += parse_amount(op.get("debit_amount"))
        credit_sum += parse_amount(op.get("credit_amount"))
    return round(debit_sum, 2), round(credit_sum, 2)


def parse_amount(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    text = text.replace("\u00a0", "").replace("\xa0", "").replace(" ", "")
    text = text.replace(",", ".")
    if text == "-" or text == "":
        return 0.0
    if "-" in text and text.count("-") == 1 and text.rsplit("-", 1)[1].isdigit() and len(text.rsplit("-", 1)[1]) == 2:
        text = text.rsplit("-", 1)[0] + "." + text.rsplit("-", 1)[1]
    try:
        return float(text)
    except ValueError:
        return 0.0


