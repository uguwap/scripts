"""
Экстрактор транзакций из PDF (экспериментальный).

Цель: для файлов в ./files/*.pdf извлечь операции (минимум: дата, дебет/кредит, назначение)
и сохранить CSV, чтобы прогнать LLM-классификацию тегов без данных из БД.
"""

from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pdfplumber


if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


DATE_RE = re.compile(r"^(?P<date>\d{2}\.\d{2}\.(?:\d{2}|\d{4}))\s+")
ACCOUNT20_RE = re.compile(r"\b\d{20}\b")
MONEY_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00a0]\d{3})*|\d+)[\.,](\d{2})(?!\d)")


@dataclass(frozen=True)
class ParsedTx:
    document_operation_date: str
    debit_amount: float
    credit_amount: float
    payer_or_recipient_name: str
    payment_purpose: str
    source_file: str


def parse_money_token(token: str) -> float:
    normalized = token.replace("\u00a0", " ").replace(" ", "").replace(",", ".")
    return float(normalized)


def find_last_two_money_tokens(text: str) -> tuple[tuple[int, int, str], tuple[int, int, str]] | None:
    matches = [(m.start(), m.end(), m.group(0)) for m in MONEY_RE.finditer(text)]
    if len(matches) < 2:
        return None
    return matches[-2], matches[-1]


def normalize_date(value: str) -> str:
    raw = value.strip()
    if re.match(r"^\d{2}\.\d{2}\.\d{2}$", raw):
        dd, mm, yy = raw.split(".")
        return f"{dd}.{mm}.20{yy}"
    return raw


def clean_purpose(text: str) -> str:
    s = re.sub(r"\s+", " ", text).strip()
    return s


def extract_counterparty_name(line: str, amounts_span_start: int) -> str:
    prefix = line[:amounts_span_start]
    accounts = list(ACCOUNT20_RE.finditer(prefix))
    if not accounts:
        return ""
    last_acc_end = accounts[-1].end()
    name = prefix[last_acc_end:].strip()
    name = re.sub(r"\s+", " ", name)
    return name


def iter_pdf_text_lines(pdf_path: Path) -> Iterable[str]:
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                yield line.strip()


def parse_transactions_from_lines(lines: Iterable[str], source_file: str) -> list[ParsedTx]:
    txs: list[ParsedTx] = []

    current_date: str | None = None
    current_debit: float | None = None
    current_credit: float | None = None
    current_counterparty: str = ""
    purpose_parts: list[str] = []

    def flush_current():
        nonlocal current_date, current_debit, current_credit, current_counterparty, purpose_parts
        if current_date and current_debit is not None and current_credit is not None:
            purpose = clean_purpose(" ".join(purpose_parts))
            if purpose:
                txs.append(
                    ParsedTx(
                        document_operation_date=current_date,
                        debit_amount=float(current_debit),
                        credit_amount=float(current_credit),
                        payer_or_recipient_name=current_counterparty,
                        payment_purpose=purpose,
                        source_file=source_file,
                    )
                )
        current_date = None
        current_debit = None
        current_credit = None
        current_counterparty = ""
        purpose_parts = []

    for raw in lines:
        if not raw:
            continue

        m = DATE_RE.match(raw)
        if m:
            flush_current()

            date_str = normalize_date(m.group("date"))
            last_two = find_last_two_money_tokens(raw)
            if not last_two:
                continue

            (a1_s, a1_e, a1_txt), (a2_s, a2_e, a2_txt) = last_two
            debit = parse_money_token(a1_txt)
            credit = parse_money_token(a2_txt)

            current_date = date_str
            current_debit = debit
            current_credit = credit
            current_counterparty = extract_counterparty_name(raw, a1_s)
            purpose_tail = raw[a2_e:].strip()
            if purpose_tail:
                purpose_parts.append(purpose_tail)
            continue

        if current_date:
            purpose_parts.append(raw)

    flush_current()
    return txs


def save_csv(txs: list[ParsedTx], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "document_operation_date",
                "debit_amount",
                "credit_amount",
                "payer_or_recipient_name",
                "payment_purpose",
                "source_file",
            ],
            delimiter=";",
        )
        writer.writeheader()
        for t in txs:
            writer.writerow(
                {
                    "document_operation_date": t.document_operation_date,
                    "debit_amount": f"{t.debit_amount:.2f}",
                    "credit_amount": f"{t.credit_amount:.2f}",
                    "payer_or_recipient_name": t.payer_or_recipient_name,
                    "payment_purpose": t.payment_purpose,
                    "source_file": t.source_file,
                }
            )


def main() -> int:
    base_dir = Path(__file__).parent
    files_dir = base_dir / "files"
    pdf_paths = sorted(files_dir.glob("*.pdf"))

    if not pdf_paths:
        print(f"PDF не найдены: {files_dir}")
        return 1

    all_txs: list[ParsedTx] = []
    for p in pdf_paths:
        lines = iter_pdf_text_lines(p)
        txs = parse_transactions_from_lines(lines, source_file=p.name)
        print(f"{p.name}: извлечено транзакций: {len(txs)}")
        all_txs.extend(txs)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = base_dir / "output" / f"extracted_transactions_{ts}.csv"
    save_csv(all_txs, out_path)
    print(f"\nCSV сохранён: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


