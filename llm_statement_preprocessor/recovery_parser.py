import re
from typing import Any, Dict, List, Optional, Tuple

from data_normalizer import normalize_amount, normalize_date, normalize_inn, normalize_kpp, normalize_account_number, normalize_bik, normalize_text_field


date_re = re.compile(r"\b\d{2}\.\d{2}\.\d{2,4}\b")
op_start_date_re = re.compile(r"^\d{2}\.\d{2}\.\d{2,4}\b")
inn_re = re.compile(r"\b\d{10}\b|\b\d{12}\b")
kpp_re = re.compile(r"\b\d{9}\b")
acc20_re = re.compile(r"\b\d{20}\b")
bik_re = re.compile(r"\b\d{9}\b")
amount_re = re.compile(r"\b\d[\d\s\u00a0\xa0.,]*\d(?:-\d{2}|\.\d{2})\b|\b\d+\b")


def recover_operations_from_batch(batch_text: str) -> List[Dict[str, Any]]:
    lines = [line for line in batch_text.split("\n") if line.strip()]
    operations: List[Dict[str, Any]] = []
    last_op: Optional[Dict[str, Any]] = None

    for line in lines:
        if is_table_row(line):
            source_line = parse_source_line(line)
            row_cells = split_table_row(line)
            op = parse_row_cells(row_cells)
            if op:
                op["source_line"] = source_line
                operations.append(op)
                last_op = op
                continue

        if last_op is not None:
            append_continuation(last_op, line)

    if operations:
        return operations
    return build_fallback_operations(batch_text)


def is_table_row(line: str) -> bool:
    if "│" not in line:
        return False
    if "└" in line or "┌" in line or "┬" in line or "┴" in line:
        return False
    return True


def split_table_row(line: str) -> List[str]:
    _, content = split_prefix(line)
    parts = [p.strip() for p in content.split("│")]
    if parts and parts[0] == "":
        parts = parts[1:]
    if parts and parts[-1] == "":
        parts = parts[:-1]
    return parts


def split_prefix(line: str) -> Tuple[str, str]:
    if line.startswith("Строка "):
        idx = line.find(":")
        if idx != -1:
            return line[: idx + 1], line[idx + 1 :].strip()
    return "", line.strip()


def parse_source_line(line: str) -> int | None:
    if not line.startswith("Строка "):
        return None
    idx = line.find(":")
    if idx == -1:
        return None
    prefix = line[:idx]
    digits = re.sub(r"[^\d]", "", prefix)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def parse_row_cells(cells: List[str]) -> Optional[Dict[str, Any]]:
    if len(cells) >= 15 and looks_like_operation_row(cells):
        return parse_by_positions(cells)
    return parse_by_regex(" ".join(cells))


def looks_like_operation_row(cells: List[str]) -> bool:
    return bool(date_re.search(cells[1])) if len(cells) > 1 else False


def parse_by_positions(cells: List[str]) -> Dict[str, Any]:
    operation_date = normalize_date(first_match(date_re, cells[1]))
    doc_type = normalize_text_field(cells[2]) if len(cells) > 2 else None
    doc_number = normalize_text_field(cells[3]) if len(cells) > 3 else None
    doc_date = normalize_date(first_match(date_re, cells[4])) if len(cells) > 4 else None

    corr = normalize_account_number(first_match(acc20_re, cells[5])) if len(cells) > 5 else None
    bank_name = normalize_text_field(cells[6]) if len(cells) > 6 else None
    bank_bik = normalize_bik(first_match(bik_re, cells[7])) if len(cells) > 7 else None

    name_cell = cells[8] if len(cells) > 8 else ""
    inn_cell = cells[9] if len(cells) > 9 else ""
    kpp_cell = cells[10] if len(cells) > 10 else ""
    account_cell = cells[11] if len(cells) > 11 else ""

    debit_cell = cells[12] if len(cells) > 12 else ""
    credit_cell = cells[13] if len(cells) > 13 else ""
    purpose_cell = cells[14] if len(cells) > 14 else ""

    payer_name = normalize_text_field(extract_name_from_mixed(name_cell))
    payer_inn = normalize_inn(first_match(inn_re, inn_cell) or first_match(inn_re, name_cell))
    payer_kpp = normalize_kpp(first_match(kpp_re, kpp_cell) or first_match(kpp_re, name_cell))
    account_number = normalize_account_number(first_match(acc20_re, account_cell))

    debit = normalize_amount(debit_cell) if debit_cell.strip() else "0"
    credit = normalize_amount(credit_cell) if credit_cell.strip() else "0"

    purpose = normalize_text_field(purpose_cell)

    return {
        "document_operation_date": operation_date,
        "document_type_code": doc_type,
        "document_number": doc_number,
        "document_date": doc_date,
        "payer_or_recipient_name": payer_name,
        "payer_or_recipient_inn": payer_inn,
        "payer_or_recipient_kpp": payer_kpp,
        "account_number": account_number,
        "debit_amount": debit,
        "credit_amount": credit,
        "payment_purpose": purpose,
        "correspondent_account_number": corr,
        "payer_or_recipient_bank": bank_name,
        "bank_bik": bank_bik,
    }


def parse_by_regex(text: str) -> Optional[Dict[str, Any]]:
    date_value = normalize_date(first_match(date_re, text))
    accounts = acc20_re.findall(text)
    amounts = extract_amounts(text)
    inn = normalize_inn(first_match(inn_re, text))
    kpp = normalize_kpp(first_match(kpp_re, text))

    debit, credit = infer_debit_credit(amounts)
    purpose = normalize_text_field(text)

    return {
        "document_operation_date": date_value,
        "document_type_code": None,
        "document_number": None,
        "document_date": None,
        "payer_or_recipient_name": None,
        "payer_or_recipient_inn": inn,
        "payer_or_recipient_kpp": kpp,
        "account_number": normalize_account_number(accounts[-1]) if accounts else None,
        "debit_amount": debit,
        "credit_amount": credit,
        "payment_purpose": purpose,
        "correspondent_account_number": normalize_account_number(first_match(acc20_re, text)),
        "payer_or_recipient_bank": None,
        "bank_bik": normalize_bik(first_match(bik_re, text)),
    }


def extract_name_from_mixed(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\bИНН\b.*$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\bКПП\b.*$", "", text, flags=re.IGNORECASE).strip()
    return text


def first_match(pattern: re.Pattern, text: str) -> Optional[str]:
    if not text:
        return None
    m = pattern.search(text)
    return m.group(0) if m else None


def extract_amounts(text: str) -> List[str]:
    values = []
    for m in amount_re.finditer(text):
        raw = m.group(0)
        raw = raw.replace("\u00a0", "").replace("\xa0", "").replace(" ", "")
        if raw:
            values.append(raw)
    return values


def infer_debit_credit(amounts: List[str]) -> Tuple[str, str]:
    if not amounts:
        return "0", "0"
    if len(amounts) == 1:
        return normalize_amount(amounts[0]), "0"
    return normalize_amount(amounts[-2]), normalize_amount(amounts[-1])


def append_continuation(op: Dict[str, Any], line: str) -> None:
    _, content = split_prefix(line)
    content = content.strip()
    if not content:
        return
    prev = op.get("payment_purpose") or ""
    combined = (prev + "\n" + content).strip() if prev else content
    op["payment_purpose"] = normalize_text_field(combined)


def raw_operation(batch_text: str) -> Dict[str, Any]:
    return {
        "source_line": None,
        "document_operation_date": None,
        "document_type_code": None,
        "document_number": None,
        "document_date": None,
        "payer_or_recipient_name": None,
        "payer_or_recipient_inn": None,
        "payer_or_recipient_kpp": None,
        "account_number": None,
        "debit_amount": "0",
        "credit_amount": "0",
        "payment_purpose": normalize_text_field(batch_text),
        "correspondent_account_number": None,
        "payer_or_recipient_bank": None,
        "bank_bik": None,
    }


def build_fallback_operations(batch_text: str) -> List[Dict[str, Any]]:
    groups = group_operations_by_start(batch_text)
    if not groups:
        return [raw_operation(batch_text)]

    operations: List[Dict[str, Any]] = []
    for group in groups:
        first = group[0] if group else ""
        source_line = parse_source_line(first)
        merged = " ".join([split_prefix(line)[1] for line in group if line.strip()])
        op = parse_by_regex(merged)
        op["source_line"] = source_line
        op["payment_purpose"] = normalize_text_field("\n".join(group))
        operations.append(op)
    return operations


def group_operations_by_start(batch_text: str) -> List[List[str]]:
    lines = [line for line in batch_text.split("\n") if line.strip()]
    groups: List[List[str]] = []
    current: List[str] = []

    for line in lines:
        if is_operation_start_line(line):
            if current:
                groups.append(current)
            current = [line]
            continue
        if current:
            current.append(line)

    if current:
        groups.append(current)
    return groups


def is_operation_start_line(line: str) -> bool:
    _, content = split_prefix(line)
    if op_start_date_re.match(content):
        return True
    if "│" in content:
        parts = [p.strip() for p in content.split("│")]
        if parts and parts[0] == "":
            parts = parts[1:]
        if parts and parts[-1] == "":
            parts = parts[:-1]
        if len(parts) >= 2 and date_re.search(parts[1] or ""):
            first_cell_digits = re.sub(r"[^\d]", "", (parts[0] or "").strip())
            return bool(first_cell_digits)
    return False


