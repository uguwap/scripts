from typing import Any

from data_normalizer import normalize_header, normalize_operations


def validate_header(header: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    h = normalize_header(header or {})

    if not h.get("debtor_name"):
        errors.append("debtor_name is empty")
    if not h.get("debtor_account_number"):
        errors.append("debtor_account_number is empty")
    if not h.get("currency_code"):
        errors.append("currency_code is empty")

    return len(errors) == 0, errors


def validate_all_operations(operations: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    normalized = normalize_operations(operations or [])
    valid: list[dict[str, Any]] = []

    for idx, op in enumerate(normalized, start=1):
        if not op.get("document_operation_date"):
            errors.append(f"op[{idx}]: missing document_operation_date")
            continue
        valid.append(op)

    return valid, errors





