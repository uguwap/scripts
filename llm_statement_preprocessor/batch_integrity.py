from dataclasses import dataclass
from typing import Any

from input_parser import parse_numbered_line, looks_like_operation_start
from recovery_parser import recover_operations_from_batch


@dataclass
class BatchIntegrityResult:
    operations: list[dict[str, Any]]
    expected: int
    extracted: int
    missing_source_lines: list[int]
    source_line_filled: int


def extract_expected_source_lines(batch_text: str) -> list[int]:
    result: list[int] = []
    for line in batch_text.split("\n"):
        if not line.strip():
            continue
        if not looks_like_operation_start(line):
            continue
        source_line, _ = parse_numbered_line(line)
        if source_line is None:
            continue
        result.append(source_line)
    return result


def operation_key(op: dict[str, Any]) -> tuple[Any, ...]:
    return (
        op.get("document_operation_date"),
        op.get("document_number"),
        op.get("debit_amount"),
        op.get("credit_amount"),
        op.get("account_number"),
        op.get("payer_or_recipient_inn"),
        op.get("payer_or_recipient_name"),
    )


def dedupe_operations(operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_source_line: dict[int, dict[str, Any]] = {}
    no_line: list[dict[str, Any]] = []

    for op in operations:
        sl = op.get("source_line")
        if isinstance(sl, int):
            by_source_line.setdefault(sl, op)
        else:
            no_line.append(op)

    seen_keys: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []

    for sl in sorted(by_source_line.keys()):
        op = by_source_line[sl]
        k = ("sl", sl)
        if k in seen_keys:
            continue
        seen_keys.add(k)
        out.append(op)

    for op in no_line:
        k = ("k",) + operation_key(op)
        if k in seen_keys:
            continue
        seen_keys.add(k)
        out.append(op)

    return out


def fill_source_line_from_recovery(
    operations: list[dict[str, Any]],
    recovered: list[dict[str, Any]],
) -> int:
    filled = 0
    index: dict[tuple[Any, ...], int] = {}

    for rop in recovered:
        sl = rop.get("source_line")
        if not isinstance(sl, int):
            continue
        k = operation_key(rop)
        if k in index:
            index[k] = -1
        else:
            index[k] = sl

    for op in operations:
        if op.get("source_line") is not None:
            continue
        sl = index.get(operation_key(op))
        if isinstance(sl, int) and sl > 0:
            op["source_line"] = sl
            filled += 1

    return filled


def ensure_batch_integrity(
    batch_text: str,
    operations: list[dict[str, Any]],
    strict_mode: bool,
    recovered: list[dict[str, Any]] | None = None,
) -> BatchIntegrityResult:
    expected_lines = extract_expected_source_lines(batch_text)
    expected_set = set(expected_lines)

    recovered_ops = recovered if recovered is not None else recover_operations_from_batch(batch_text)
    filled = fill_source_line_from_recovery(operations, recovered_ops)

    extracted_lines = {op.get("source_line") for op in operations if isinstance(op.get("source_line"), int)}
    missing = sorted([sl for sl in expected_set if sl not in extracted_lines])

    if missing:
        recovered_by_line = {op.get("source_line"): op for op in recovered_ops if isinstance(op.get("source_line"), int)}
        for sl in missing:
            rop = recovered_by_line.get(sl)
            if rop is not None:
                operations.append(rop)
        operations = dedupe_operations(operations)

    extracted_lines = {op.get("source_line") for op in operations if isinstance(op.get("source_line"), int)}
    missing_after = sorted([sl for sl in expected_set if sl not in extracted_lines])

    if strict_mode and missing_after:
        operations = dedupe_operations(recovered_ops)
        missing_after = []

    return BatchIntegrityResult(
        operations=operations,
        expected=len(expected_lines),
        extracted=len(operations),
        missing_source_lines=missing_after,
        source_line_filled=filled,
    )


