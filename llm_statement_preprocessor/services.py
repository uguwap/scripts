from dataclasses import dataclass, field
from typing import List, Dict, Any
from pathlib import Path

from config import Config
from logger import log_step, log_error
from input_parser import read_unstructured_file, count_operations_in_text
from llm_client import LLMClient
from excel_builder import build_excel_file
from run_logs import RunLogs
from recovery_parser import recover_operations_from_batch
import re
from data_normalizer import normalize_date
from batch_integrity import ensure_batch_integrity
from data_normalizer import normalize_operations


@dataclass
class CheckpointInfo:
    batch_range: str
    file_path: str
    operations_count: int


@dataclass
class ProcessingResult:
    header: Dict[str, Any]
    operations: List[Dict[str, Any]]
    total_operations: int
    valid_operations: int
    header_warnings: List[str]
    operation_errors: List[str]
    checkpoints: List[CheckpointInfo] = field(default_factory=list)
    
    @property
    def is_success(self) -> bool:
        return len(self.operations) > 0
    
    @property
    def stats(self) -> Dict[str, int]:
        return {
            "total_operations": self.total_operations,
            "valid_operations": self.valid_operations,
            "invalid_operations": self.total_operations - self.valid_operations,
            "checkpoints_created": len(self.checkpoints)
        }


class StatementPreprocessor:
    
    def __init__(self, config: Config):
        self.config = config
        self.llm_enabled = bool(config.llm_enabled)
        self.llm_client = LLMClient(config) if self.llm_enabled else None
    
    def process_file(
        self, 
        file_path: str, 
        checkpoint_dir: str = None,
        checkpoint_every: int = 50,
        strict_mode: bool = True,
        run_logs: RunLogs | None = None,
    ) -> ProcessingResult:
        log_step("PROCESSING_START", file=file_path, checkpoints_enabled=bool(checkpoint_dir))
        
        file_data = read_unstructured_file(file_path, batch_size=self.config.batch_size)
        if run_logs:
            run_logs.log_source_text(file_data["header"], "\n".join(file_data["operations_batches"]))

        if self.llm_enabled:
            self.llm_client = LLMClient(self.config, run_logs=run_logs)
        else:
            self.llm_client = None

        header = self.extract_header(file_data["header"])
        log_step("HEADER_EXTRACTED", fields=list(header.keys()) if header else [])
            
        try:
            operations, checkpoints = self.extract_operations(
                file_data["operations_batches"],
                header=header,
                checkpoint_dir=checkpoint_dir,
                checkpoint_every=checkpoint_every,
                strict_mode=strict_mode,
                run_logs=run_logs,
            )
            
            log_step("OPERATIONS_EXTRACTED", total=len(operations))
            
            result = ProcessingResult(
                header=header,
                operations=operations,
                total_operations=len(operations),
                valid_operations=len(operations),
                header_warnings=[],
                operation_errors=[],
                checkpoints=checkpoints
            )
            
            log_step("PROCESSING_COMPLETE", 
                     success=result.is_success, 
                     total_operations=len(operations),
                     checkpoints=len(checkpoints))
            return result
            
        except Exception as e:
            log_error(e, "Processing failed")
            raise
    
    def extract_header(self, header_text: str) -> Dict[str, Any]:
        if not self.llm_enabled or self.llm_client is None:
            log_step("HEADER_SKIPPED", reason="llm_disabled")
            from data_normalizer import normalize_header
            return normalize_header({})
        from data_normalizer import normalize_header
        try:
            return self.llm_client.extract_header(header_text)
        except Exception as e:
            if looks_like_llm_quota_error(e):
                self.llm_enabled = False
                self.llm_client = None
                log_step("LLM_DISABLED", reason="quota_or_balance", error_preview=str(e)[:200])
                log_step("HEADER_SKIPPED", reason="llm_quota_or_balance")
                return normalize_header({})
            log_error(e, "HEADER_EXTRACTION_FAILED")
            return normalize_header({})
    
    
    def extract_operations(
        self, 
        batches: List[str], 
        header: Dict[str, Any] = None,
        checkpoint_dir: str = None,
        checkpoint_every: int = 50,
        strict_mode: bool = True,
        run_logs: RunLogs | None = None,
    ) -> tuple[List[Dict[str, Any]], List[CheckpointInfo]]:
        all_operations = []
        checkpoints = []
        failed_batches = []
        llm_disabled_reason: str | None = None
        
        for batch_idx, batch_text in enumerate(batches, start=1):
            log_step("BATCH_PROCESSING", batch=batch_idx, total_batches=len(batches))
            
            try:
                expected = count_operations_in_text(batch_text)
                log_step("BATCH_EXPECTED_OPS", batch=batch_idx, expected_ops=expected, strict_mode=strict_mode)
                if not self.llm_enabled or self.llm_client is None:
                    recovered = recover_operations_from_batch(batch_text)
                    recovery_integrity = ensure_batch_integrity(batch_text, recovered, strict_mode=strict_mode, recovered=recovered)
                    chosen, meta = self.choose_batch_result(expected, None, recovery_integrity.operations, strict_mode=strict_mode)
                    meta.update(
                        {
                            "missing_source_lines": len(recovery_integrity.missing_source_lines),
                            "source_line_filled": recovery_integrity.source_line_filled,
                        }
                    )
                    all_operations.extend(chosen)
                    if run_logs:
                        run_logs.log_batch_recovery(batch_idx, chosen, "llm_disabled")
                        run_logs.log_batch_selection(batch_idx, meta)
                    log_step("BATCH_SELECTED", batch=batch_idx, **meta)
                    continue

                llm_ops_raw = self.llm_client.extract_operations(batch_text, batch_number=batch_idx)
                recovered_raw = recover_operations_from_batch(batch_text)

                recovery_integrity = ensure_batch_integrity(batch_text, recovered_raw, strict_mode=strict_mode, recovered=recovered_raw)
                chosen_raw, meta = self.choose_batch_result(
                    expected,
                    llm_ops_raw,
                    chosen_recovery=recovery_integrity.operations,
                    strict_mode=strict_mode,
                )

                llm_failed = bool(expected) and (not llm_ops_raw)
                if llm_failed:
                    meta["selected"] = "recovery"
                    meta["reason"] = "llm_invalid_json_or_empty"
                    chosen_raw = recovery_integrity.operations

                chosen_integrity = ensure_batch_integrity(batch_text, chosen_raw, strict_mode=strict_mode, recovered=recovered_raw)
                meta.update(
                    {
                        "llm_raw_count": len(llm_ops_raw),
                        "recovery_raw_count": len(recovered_raw),
                        "llm_failed": llm_failed,
                        "missing_source_lines": len(chosen_integrity.missing_source_lines),
                        "source_line_filled": chosen_integrity.source_line_filled,
                        "ops_count": len(chosen_integrity.operations),
                    }
                )

                all_operations.extend(chosen_integrity.operations)
                if run_logs:
                    if meta.get("selected") == "recovery":
                        run_logs.log_batch_recovery(batch_idx, chosen_integrity.operations, meta.get("reason") or "selected_recovery")
                    run_logs.log_batch_selection(batch_idx, meta)
                log_step("BATCH_SELECTED", batch=batch_idx, **meta)
                
            except Exception as e:
                log_error(e, f"⚠️  Batch {batch_idx} extraction failed with exception")
                if self.llm_enabled and looks_like_llm_quota_error(e):
                    self.llm_enabled = False
                    llm_disabled_reason = str(e)[:200]
                    log_step("LLM_DISABLED", reason="quota_or_balance", error_preview=llm_disabled_reason)
                recovered = recover_operations_from_batch(batch_text)
                recovery_integrity = ensure_batch_integrity(batch_text, recovered, strict_mode=strict_mode, recovered=recovered)
                chosen, meta = self.choose_batch_result(count_operations_in_text(batch_text), None, recovery_integrity.operations, strict_mode=strict_mode)
                meta.update(
                    {
                        "missing_source_lines": len(recovery_integrity.missing_source_lines),
                        "source_line_filled": recovery_integrity.source_line_filled,
                    }
                )
                all_operations.extend(chosen)
                failed_batches.append(
                    {
                        "batch_number": batch_idx,
                        "text_preview": batch_text[:500],
                        "reason": f"Exception: {str(e)}",
                        "recovered_ops": len(chosen),
                    }
                )
                if run_logs:
                    run_logs.log_batch_recovery(batch_idx, chosen, f"exception:{type(e).__name__}")
                    run_logs.log_batch_selection(batch_idx, {**meta, "selected": "recovery", "reason": f"exception:{type(e).__name__}"})
                log_step("BATCH_SELECTED", batch=batch_idx, selected="recovery", reason=f"exception:{type(e).__name__}", ops_count=len(chosen), expected_ops=count_operations_in_text(batch_text), coverage=meta.get("coverage"))
            
            if checkpoint_dir and header and batch_idx % checkpoint_every == 0:
                checkpoint = self.save_checkpoint(
                    header, 
                    all_operations, 
                    checkpoint_dir, 
                    batch_idx - checkpoint_every + 1, 
                    batch_idx
                )
                checkpoints.append(checkpoint)
                log_step("CHECKPOINT_SAVED", batch_range=checkpoint.batch_range, ops_count=len(all_operations))
        
        if failed_batches:
            log_step("CRITICAL_FAILURES", 
                     total_failed=len(failed_batches), 
                     failed_batch_numbers=[b["batch_number"] for b in failed_batches])
            
            if checkpoint_dir:
                self.save_failed_batches_report(failed_batches, checkpoint_dir)
            if run_logs:
                run_logs.log_summary(
                    {
                        "status": "failed_batches_present",
                        "failed_batches": failed_batches,
                        "total_batches": len(batches),
                        "checkpoint_every": checkpoint_every,
                        "operations_extracted": len(all_operations),
                        "llm_disabled_reason": llm_disabled_reason,
                    }
                )
        all_operations = self.finalize_operations(all_operations)
        self.log_quality_summary(batches, all_operations, strict_mode=strict_mode)
        
        return all_operations, checkpoints

    def choose_batch_result(
        self,
        expected_ops: int,
        llm_ops: list[dict[str, Any]] | None,
        chosen_recovery: list[dict[str, Any]],
        strict_mode: bool,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        llm_score = self.score_operations(expected_ops, llm_ops or [])
        recovery_score = self.score_operations(expected_ops, chosen_recovery)

        selected = "llm" if llm_score["score"] >= recovery_score["score"] else "recovery"
        chosen = (llm_ops or []) if selected == "llm" else chosen_recovery
        chosen_score = llm_score if selected == "llm" else recovery_score

        if strict_mode and expected_ops and chosen_score["coverage"] < 0.95:
            selected = "recovery"
            chosen = chosen_recovery
            chosen_score = recovery_score

        meta = {
            "selected": selected,
            "expected_ops": expected_ops,
            "ops_count": len(chosen),
            "coverage": chosen_score["coverage"],
            "with_source_line": chosen_score["with_source_line"],
            "with_date": chosen_score["with_date"],
            "score": chosen_score["score"],
            "reason": chosen_score["reason"],
            "llm_score": llm_score["score"],
            "recovery_score": recovery_score["score"],
        }
        return chosen, meta

    def score_operations(self, expected_ops: int, ops: list[dict[str, Any]]) -> dict[str, Any]:
        with_source_line = sum(1 for op in ops if op.get("source_line") is not None)
        with_date = sum(1 for op in ops if op.get("document_operation_date"))
        has_any = 1 if ops else 0
        coverage = 1.0
        if expected_ops:
            coverage = min(1.0, len(ops) / max(1, expected_ops))

        score = (
            has_any * 10
            + coverage * 10
            + (with_source_line / max(1, len(ops))) * 5
            + (with_date / max(1, len(ops))) * 3
        )

        reason = "ok"
        if not ops:
            reason = "empty"
        elif expected_ops and coverage < 0.5:
            reason = "low_coverage"
        elif with_source_line == 0:
            reason = "no_source_line"

        return {
            "score": round(float(score), 3),
            "coverage": round(float(coverage), 3),
            "with_source_line": with_source_line,
            "with_date": with_date,
            "reason": reason,
        }

    def finalize_operations(self, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        operations = normalize_operations(operations)
        def key(op: dict[str, Any]) -> tuple[int, str, str, str]:
            sl = op.get("source_line")
            sl_key = int(sl) if isinstance(sl, int) else 10**12
            return (
                sl_key,
                str(op.get("document_operation_date") or ""),
                str(op.get("document_number") or ""),
                str(op.get("debit_amount") or "") + "|" + str(op.get("credit_amount") or ""),
            )

        seen: set[tuple[int, str, str, str]] = set()
        out: list[dict[str, Any]] = []
        for op in sorted(operations, key=key):
            k = key(op)
            if k in seen:
                continue
            seen.add(k)
            out.append(op)
        return out

    def log_quality_summary(self, batches: List[str], operations: List[Dict[str, Any]], strict_mode: bool) -> None:
        expected_total = sum(count_operations_in_text(b) for b in batches)
        source_lines = {op.get("source_line") for op in operations if isinstance(op.get("source_line"), int)}
        date_in_source = set()
        date_re = re.compile(r"\b\d{2}\.\d{2}\.\d{2,4}\b")
        for b in batches:
            for m in date_re.findall(b):
                date_in_source.add(normalize_date(m))

        extracted_dates = set()
        for op in operations:
            d = op.get("document_operation_date")
            if d:
                extracted_dates.add(normalize_date(d))

        missing_dates = sorted([d for d in date_in_source if d and d not in extracted_dates])
        log_step(
            "QUALITY_SUMMARY",
            strict_mode=strict_mode,
            expected_total_ops=expected_total,
            unique_source_lines=len(source_lines),
            source_dates=len(date_in_source),
            extracted_dates=len(extracted_dates),
            missing_dates_preview=missing_dates[:20],
        )

    def save_failed_batches_report(self, failed_batches: List[Dict], output_dir: str):
        import json
        from datetime import datetime
        
        report_path = Path(output_dir) / f"FAILED_BATCHES_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "total_failed": len(failed_batches),
                    "timestamp": datetime.now().isoformat(),
                    "batches": failed_batches,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        
        log_step("FAILED_BATCHES_REPORT_SAVED", path=str(report_path))

    def save_checkpoint(
        self, 
        header: Dict[str, Any], 
        operations: List[Dict[str, Any]], 
        output_dir: str, 
        start_batch: int, 
        end_batch: int
    ) -> CheckpointInfo:
        batch_range = f"{start_batch}-{end_batch}"
        filename = f"checkpoint_{batch_range}.xlsx"
        file_path = Path(output_dir) / filename
        
        build_excel_file(header, operations, str(file_path), batch_info=batch_range)
        
        return CheckpointInfo(
            batch_range=batch_range,
            file_path=str(file_path),
            operations_count=len(operations)
        )

    def save_to_excel(self, result: ProcessingResult, output_path: str) -> None:
        from excel_builder import build_standard_excel
        build_standard_excel(
            header=result.header,
            operations=result.operations,
            output_path=output_path
        )


def looks_like_llm_quota_error(err: Exception) -> bool:
    text = str(err)
    if "Error code: 402" in text:
        return True
    if "Insufficient Balance" in text:
        return True
    lowered = text.lower()
    if "insufficient" in lowered and "balance" in lowered:
        return True
    if "quota" in lowered:
        return True
    return False


class PreprocessorFactory:
    
    @staticmethod
    def create_from_env() -> StatementPreprocessor:
        config = Config.from_env()
        config.validate()
        return StatementPreprocessor(config)
