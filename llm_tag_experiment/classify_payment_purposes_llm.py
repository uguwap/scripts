from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from openai import OpenAI
except ImportError:
    print("Устанавливаю openai...")
    os.system(f"{sys.executable} -m pip install openai --quiet")
    from openai import OpenAI


if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


NEOPR = "НЕ ОПРЕДЕЛЕНО"
DEFAULT_IN_DIR = Path(__file__).parent / "output"
DEFAULT_OUT_DIR = Path(__file__).parent / "output"

ALLOWED_L1 = {"Incoming", "Outgoing"}
ALLOWED_L2 = {
    "Customers",
    "OwnTransfer",
    "CashDeposit",
    "Loans",
    "Refunds",
    "Suppliers",
    "Leasing",
    "CreditRepay",
    "LoanRepay",
    "CommFinRepay",
    "LoansIssued",
    "CashWithdraw",
    "BankFees",
    "Taxes",
    "Other",
}
ALLOWED_L3 = {
    "Transport",
    "Platon",
    "Fuel",
    "Rent",
    "Repair",
    "Goods",
    "Services",
    "FromFounders",
    "FromLegal",
    "BankCredit",
    "Factoring",
    "Accountable",
    "ErrorPayment",
    "CashOut",
    "Affiliated",
    "Other",
    "",
}


@dataclass(frozen=True)
class LlmCfg:
    provider: str
    api_key: str
    base_url: str
    model: str
    temperature: float
    max_tokens: int
    batch_size: int


def load_env_file_if_exists(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and str(value).strip() != "" else default


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def get_llm_cfg() -> LlmCfg:
    load_env_file_if_exists(Path(__file__).parent / ".env")
    load_env_file_if_exists(Path(__file__).resolve().parents[2] / ".env")
    provider = env("LLM_PROVIDER", "deepseek").strip().lower()
    default_model = "deepseek-chat" if provider == "deepseek" else "gpt-4o-mini"
    api_key = env("LLM_API_KEY", "")
    if not api_key:
        raise ValueError("Не задан LLM_API_KEY. Установите переменную окружения и повторите запуск.")
    return LlmCfg(
        provider=provider,
        api_key=api_key,
        base_url=env("LLM_BASE_URL", ""),
        model=env("LLM_MODEL", default_model),
        temperature=float(env("LLM_TEMPERATURE", "0")),
        max_tokens=int(env("LLM_MAX_TOKENS", "4000")),
        batch_size=int(env("LLM_BATCH_SIZE", "25")),
    )


def build_client(cfg: LlmCfg) -> OpenAI:
    if cfg.base_url:
        base_url = cfg.base_url
    elif cfg.provider == "deepseek":
        base_url = "https://api.deepseek.com"
    else:
        base_url = None
    return OpenAI(api_key=cfg.api_key, base_url=base_url)


def extract_json_substring(text: str) -> str | None:
    text = (text or "").strip()
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    start = min((i for i in [text.find("["), text.find("{")] if i >= 0), default=-1)
    end = max(text.rfind("]"), text.rfind("}"))
    if start < 0 or end < 0 or end <= start:
        return None
    return text[start : end + 1]


def chunk_list(items: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def build_prompt(batch: list[dict[str, str]]) -> str:
    payload = json.dumps(batch, ensure_ascii=False)
    return (
        "Классифицируй назначения платежей в формате level1-level2-level3.\n"
        "Верни ТОЛЬКО JSON-массив объектов такого вида:\n"
        "[{\"id\":\"...\",\"llm_level1\":\"...\",\"llm_level2\":\"...\",\"llm_level3\":\"...\"}]\n\n"
        "Правила:\n"
        "1) llm_level1 строго из: Incoming, Outgoing, НЕ ОПРЕДЕЛЕНО.\n"
        "2) llm_level2 строго из словаря категорий или НЕ ОПРЕДЕЛЕНО.\n"
        "3) llm_level3 строго из словаря подкатегорий или пустая строка.\n"
        "4) Если не уверен, ставь llm_level1=НЕ ОПРЕДЕЛЕНО и llm_level2=НЕ ОПРЕДЕЛЕНО.\n"
        "5) Не придумывай данные, не меняй id.\n\n"
        "Словарь level2:\n"
        "Customers, OwnTransfer, CashDeposit, Loans, Refunds, Suppliers, Leasing, CreditRepay, "
        "LoanRepay, CommFinRepay, LoansIssued, CashWithdraw, BankFees, Taxes, Other.\n"
        "Словарь level3:\n"
        "Transport, Platon, Fuel, Rent, Repair, Goods, Services, FromFounders, FromLegal, "
        "BankCredit, Factoring, Accountable, ErrorPayment, CashOut, Affiliated, Other.\n\n"
        f"Данные для классификации:\n{payload}"
    )


def sanitize_label(level1: str, level2: str, level3: str) -> tuple[str, str, str]:
    l1 = normalize_ws(level1)
    l2 = normalize_ws(level2)
    l3 = normalize_ws(level3)

    if l1 not in ALLOWED_L1:
        l1 = NEOPR
    if l2 not in ALLOWED_L2:
        l2 = NEOPR if l1 == NEOPR else "Other"
    if l3 not in ALLOWED_L3:
        l3 = ""
    if l1 == NEOPR:
        l2 = NEOPR
        l3 = ""
    return l1, l2, l3


def classify_batch(client: OpenAI, cfg: LlmCfg, batch: list[dict[str, str]]) -> list[dict[str, str]]:
    system_prompt = (
        "Ты эксперт по типизации банковских платежей для корпоративных выписок. "
        "Отвечай только валидным JSON без markdown."
    )
    user_prompt = build_prompt(batch)
    response = client.chat.completions.create(
        model=cfg.model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=cfg.temperature,
        max_tokens=cfg.max_tokens,
    )
    content = response.choices[0].message.content or ""
    json_text = extract_json_substring(content)
    if not json_text:
        raise ValueError("LLM вернул ответ без JSON.")
    parsed = json.loads(json_text)
    if not isinstance(parsed, list):
        raise ValueError("LLM вернул не массив.")
    output: list[dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        tx_id = normalize_ws(str(item.get("id", "")))
        l1, l2, l3 = sanitize_label(
            str(item.get("llm_level1", "")),
            str(item.get("llm_level2", "")),
            str(item.get("llm_level3", "")),
        )
        if tx_id:
            output.append({"id": tx_id, "llm_level1": l1, "llm_level2": l2, "llm_level3": l3})
    return output


def classify_unique_purposes(purposes: list[str], cfg: LlmCfg) -> dict[str, dict[str, str]]:
    client = build_client(cfg)
    items = [{"id": str(i), "payment_purpose": p} for i, p in enumerate(purposes, start=1)]
    result_map: dict[str, dict[str, str]] = {}

    batches = chunk_list(items, max(1, cfg.batch_size))
    for idx, batch in enumerate(batches, start=1):
        print(f"Батч {idx}/{len(batches)}: {len(batch)} назначений...")
        parsed_batch: list[dict[str, str]] | None = None
        last_error = None

        for attempt in range(1, 4):
            try:
                parsed_batch = classify_batch(client, cfg, batch)
                if parsed_batch:
                    break
            except Exception as exc:
                last_error = exc
                time.sleep(min(2**attempt, 8))

        if parsed_batch is None:
            print(f"  Ошибка батча: {last_error}")
            parsed_batch = []

        by_id = {row["id"]: row for row in parsed_batch}
        for item in batch:
            row = by_id.get(item["id"])
            if not row:
                result_map[item["payment_purpose"]] = {
                    "llm_level1": NEOPR,
                    "llm_level2": NEOPR,
                    "llm_level3": "",
                }
                continue
            result_map[item["payment_purpose"]] = {
                "llm_level1": row["llm_level1"],
                "llm_level2": row["llm_level2"],
                "llm_level3": row["llm_level3"],
            }

    return result_map


def read_semicolon_csv(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        return [dict(row) for row in reader]


def build_rows_for_export(rows: list[dict[str, str]], purpose_map: dict[str, dict[str, str]]) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    xlsx_rows: list[dict[str, Any]] = []
    csv_rows: list[dict[str, str]] = []

    for idx, row in enumerate(rows, start=1):
        purpose = normalize_ws(row.get("payment_purpose", ""))
        labels = purpose_map.get(
            purpose,
            {"llm_level1": NEOPR, "llm_level2": NEOPR, "llm_level3": ""},
        )

        debit = normalize_ws(row.get("debit_amount", "0"))
        credit = normalize_ws(row.get("credit_amount", "0"))
        source_file = normalize_ws(row.get("source_file", ""))

        xlsx_rows.append(
            {
                "document_number": "",
                "document_operation_date": normalize_ws(row.get("document_operation_date", "")),
                "payer_or_recipient_name": normalize_ws(row.get("payer_or_recipient_name", "")),
                "payer_or_recipient_inn": "",
                "counterparty_okved": "",
                "is_leasing_company": "",
                "debtor_name": "",
                "debtor_inn": "",
                "debit_amount": debit,
                "credit_amount": credit,
                "payment_purpose": purpose,
                "tag": "Иное",
                "llm_level_1": labels["llm_level1"],
                "llm_level_2": labels["llm_level2"],
                "llm_level_3": labels["llm_level3"],
                "source_file": source_file,
                "row_idx": idx,
            }
        )
        csv_rows.append(
            {
                "document_number": "",
                "document_operation_date": normalize_ws(row.get("document_operation_date", "")),
                "payer_or_recipient_name": normalize_ws(row.get("payer_or_recipient_name", "")),
                "payer_or_recipient_inn": "",
                "debit_amount": debit,
                "credit_amount": credit,
                "payment_purpose": purpose,
                "existing_tag": "Иное",
                "is_leasing_company": "",
                "counterparty_main_okved": "",
                "llm_level1": labels["llm_level1"],
                "llm_level2": labels["llm_level2"],
                "llm_level3": labels["llm_level3"],
                "source_file": source_file,
            }
        )
    return pd.DataFrame(xlsx_rows), csv_rows


def write_semicolon_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    in_path_env = os.getenv("EXTRACTED_CSV_PATH", "").strip()
    if in_path_env:
        in_path = Path(in_path_env).resolve()
    else:
        candidates = sorted(DEFAULT_IN_DIR.glob("extracted_transactions_*.csv"))
        if not candidates:
            print("Не найден входной CSV extracted_transactions_*.csv")
            return 2
        in_path = candidates[-1]

    if not in_path.exists():
        print(f"Входной CSV не найден: {in_path}")
        return 2

    print(f"Входной файл: {in_path}")
    rows = read_semicolon_csv(in_path)
    if not rows:
        print("Входной CSV пустой.")
        return 2

    unique_purposes = sorted({normalize_ws(r.get("payment_purpose", "")) for r in rows if normalize_ws(r.get("payment_purpose", ""))})
    print(f"Транзакций: {len(rows)} | Уникальных назначений: {len(unique_purposes)}")

    cfg = get_llm_cfg()
    print(f"LLM provider/model: {cfg.provider}/{cfg.model}")
    purpose_map = classify_unique_purposes(unique_purposes, cfg)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(env("EXPERIMENT_OUT_DIR", str(DEFAULT_OUT_DIR))).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    xlsx_path = out_dir / f"classification_from_files_{ts}.xlsx"
    csv_path = out_dir / f"result_adaptive_from_files_{ts}.csv"

    xlsx_df, csv_rows = build_rows_for_export(rows, purpose_map)
    xlsx_df.to_excel(xlsx_path, index=False, engine="openpyxl")
    write_semicolon_csv(csv_path, csv_rows)

    defined = int((xlsx_df["llm_level_1"] != NEOPR).sum())
    print(f"Сохранено XLSX: {xlsx_path}")
    print(f"Сохранено CSV:  {csv_path}")
    print(f"Классифицировано (не '{NEOPR}'): {defined}/{len(xlsx_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
