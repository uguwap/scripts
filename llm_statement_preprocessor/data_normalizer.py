import re
from typing import Dict, Any, List


def normalize_inn(value: Any) -> str | None:
    if value is None:
        return None
    
    text = str(value).strip()
    digits = re.sub(r'[^\d]', '', text)
    
    if len(digits) in (10, 12):
        return digits
    return text


def normalize_account_number(value: Any) -> str | None:
    if value is None:
        return None
    
    text = str(value).strip()
    digits = re.sub(r'[^\d]', '', text)
    
    if len(digits) == 20:
        return digits
    return text


def normalize_bik(value: Any) -> str | None:
    if value is None:
        return None
    
    text = str(value).strip()
    digits = re.sub(r'[^\d]', '', text)
    
    if len(digits) == 9:
        return digits
    return text


def normalize_kpp(value: Any) -> str | None:
    if value is None:
        return None
    
    text = str(value).strip()
    digits = re.sub(r'[^\d]', '', text)
    
    if len(digits) == 9:
        return digits
    return text


def normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    
    text = str(value).strip()
    
    match = re.match(r'(\d{2})\.(\d{2})\.(\d{2})$', text)
    if match:
        day, month, year = match.groups()
        return f"{day}.{month}.20{year}"
    
    if re.match(r'\d{2}\.\d{2}\.\d{4}$', text):
        return text
    
    return text


def normalize_amount(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, (int, float)):
        return str(value)

    text = str(value).strip()
    text = text.replace("\u00a0", "").replace("\xa0", "").replace(" ", "")
    text = text.replace(",", ".")
    if text.strip() == "-":
        return "0"
    if re.match(r"^\d+(\.\d+)?-\d{2}$", text):
        text = re.sub(r"-(\d{2})$", r".\1", text)
    return text


def normalize_source_line(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def normalize_text_field(value: Any) -> str | None:
    if value is None:
        return None
    
    text = str(value).strip()
    text = re.sub(r'\s+', ' ', text)
    
    return text if text else None


def normalize_header(header: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "debtor_name": normalize_text_field(header.get("debtor_name")),
        "debtor_inn": normalize_inn(header.get("debtor_inn")),
        "debtor_account_number": normalize_account_number(header.get("debtor_account_number")),
        "debtor_bank_name": normalize_text_field(header.get("debtor_bank_name")),
        "currency_code": normalize_text_field(header.get("currency_code")) or "643"
    }


def normalize_operation(operation: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {
        "source_line": normalize_source_line(operation.get("source_line")),
        "document_operation_date": normalize_date(operation.get("document_operation_date")),
        "document_type_code": normalize_text_field(operation.get("document_type_code")),
        "document_number": normalize_text_field(operation.get("document_number")),
        "document_date": normalize_date(operation.get("document_date")),
        "payer_or_recipient_name": normalize_text_field(operation.get("payer_or_recipient_name")),
        "payer_or_recipient_inn": normalize_inn(operation.get("payer_or_recipient_inn")),
        "payer_or_recipient_kpp": normalize_kpp(operation.get("payer_or_recipient_kpp")),
        "account_number": normalize_account_number(operation.get("account_number")),
        "debit_amount": normalize_amount(operation.get("debit_amount")),
        "credit_amount": normalize_amount(operation.get("credit_amount")),
        "payment_purpose": normalize_text_field(operation.get("payment_purpose")),
        "bank_bik": normalize_bik(operation.get("bank_bik")),
        "correspondent_account_number": normalize_account_number(operation.get("correspondent_account_number")),
        "payer_or_recipient_bank": normalize_text_field(operation.get("payer_or_recipient_bank"))
    }
    
    return normalized


def normalize_operations(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [normalize_operation(op) for op in operations if op]

