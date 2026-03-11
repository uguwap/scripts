from __future__ import annotations

import csv
import hashlib
import json
import math
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

EMBED_DIM = 256


def normalize_purpose_light(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", "<date>", s)
    s = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "<date>", s)
    s = re.sub(r"\b\d{5,}\b", "<num>", s)
    return s.strip()


def normalize_purpose_strong(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\d+", "<n>", s)
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def stable_hash(text: str) -> bytes:
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).digest()


def char_ngrams(text: str, n: int):
    if len(text) < n:
        return []
    return (text[i : i + n] for i in range(len(text) - n + 1))


def hashed_char_ngram_embedding(text: str, dim: int = EMBED_DIM) -> list[float]:
    text = normalize_purpose_light(text)
    if not text:
        return [0.0] * dim
    vec = [0.0] * dim
    for n in (3, 4, 5):
        for ng in char_ngrams(text, n):
            h = stable_hash(ng)
            idx = int.from_bytes(h[:4], "little") % dim
            sign = 1.0 if (h[4] & 1) else -1.0
            vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec))
    if norm <= 0:
        return vec
    return [v / norm for v in vec]


def cosine_sim(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


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
    base = Path(__file__).resolve()
    load_env_file_if_exists(base.parent / ".env")
    load_env_file_if_exists(base.parents[1] / ".env")
    load_env_file_if_exists(base.parents[2] / ".env")
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
        batch_size=int(env("LLM_BATCH_SIZE", "10")),
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
    prompt_len = len(user_prompt)
    print(f"    [debug] Отправляю запрос к {cfg.model} | prompt {prompt_len} символов...")

    t0 = time.time()
    response = client.chat.completions.create(
        model=cfg.model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=cfg.temperature,
        max_tokens=cfg.max_tokens,
    )
    elapsed = time.time() - t0

    content = response.choices[0].message.content or ""
    usage = response.usage
    tok_info = ""
    if usage:
        tok_info = f" | tokens: prompt={usage.prompt_tokens}, completion={usage.completion_tokens}"
    print(f"    [debug] Ответ за {elapsed:.1f}с | {len(content)} символов{tok_info}")

    json_text = extract_json_substring(content)
    if not json_text:
        print(f"    [debug] Не найден JSON. Первые 300 символов ответа: {content[:300]}")
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
    print(f"    [debug] Распознано {len(output)}/{len(batch)} назначений")
    return output


def classify_unique_purposes(purposes: list[str], cfg: LlmCfg) -> dict[str, dict[str, str]]:
    client = build_client(cfg)
    items = [{"id": str(i), "payment_purpose": p} for i, p in enumerate(purposes, start=1)]
    result_map: dict[str, dict[str, str]] = {}

    batches = chunk_list(items, max(1, cfg.batch_size))
    total_batches = len(batches)
    total_ok = 0
    total_fail = 0
    t_start = time.time()

    print(f"\n{'='*60}")
    print(f"Запуск классификации: {len(purposes)} назначений → {total_batches} батчей по {cfg.batch_size}")
    print(f"{'='*60}\n")

    for idx, batch in enumerate(batches, start=1):
        batch_t0 = time.time()
        print(f"--- Батч {idx}/{total_batches}: {len(batch)} назначений ---")
        parsed_batch: list[dict[str, str]] | None = None
        last_error = None

        for attempt in range(1, 4):
            try:
                parsed_batch = classify_batch(client, cfg, batch)
                if parsed_batch:
                    break
            except Exception as exc:
                last_error = exc
                wait = min(2**attempt, 8)
                print(f"    [retry] Попытка {attempt}/3 не удалась: {exc}")
                print(f"    [retry] Жду {wait}с перед повтором...")
                time.sleep(wait)

        batch_elapsed = time.time() - batch_t0

        if parsed_batch is None:
            print(f"    [FAIL] Батч провален после 3 попыток: {last_error}")
            parsed_batch = []

        matched = 0
        by_id = {row["id"]: row for row in parsed_batch}
        for item in batch:
            row = by_id.get(item["id"])
            if not row:
                result_map[item["payment_purpose"]] = {
                    "llm_level1": NEOPR,
                    "llm_level2": NEOPR,
                    "llm_level3": "",
                }
                total_fail += 1
                continue
            result_map[item["payment_purpose"]] = {
                "llm_level1": row["llm_level1"],
                "llm_level2": row["llm_level2"],
                "llm_level3": row["llm_level3"],
            }
            matched += 1
            total_ok += 1

        elapsed_total = time.time() - t_start
        eta = (elapsed_total / idx) * (total_batches - idx) if idx < total_batches else 0
        print(f"    Батч за {batch_elapsed:.1f}с | OK: {matched}/{len(batch)} | "
              f"Прогресс: {idx}/{total_batches} | Общее время: {elapsed_total:.0f}с | ETA: {eta:.0f}с\n")

    elapsed_all = time.time() - t_start
    print(f"{'='*60}")
    print(f"Классификация завершена за {elapsed_all:.0f}с")
    print(f"Успешно: {total_ok} | Не распознано: {total_fail} | Всего: {total_ok + total_fail}")
    print(f"{'='*60}\n")

    return result_map


def read_semicolon_csv(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        return [dict(row) for row in reader]


def load_reference_corpus(path: Path) -> list[dict[str, Any]]:
    rows = read_semicolon_csv(path)
    by_strong: dict[str, dict[str, Any]] = {}
    for row in rows:
        purpose = normalize_ws(row.get("payment_purpose", ""))
        if not purpose:
            continue
        strong = normalize_purpose_strong(purpose)
        if strong in by_strong:
            continue
        emb = hashed_char_ngram_embedding(purpose)
        l1 = normalize_ws(row.get("llm_level1", ""))
        l2 = normalize_ws(row.get("llm_level2", ""))
        l3 = normalize_ws(row.get("llm_level3", ""))
        if l1 == NEOPR and not l2:
            continue
        by_strong[strong] = {
            "purpose_norm_strong": strong,
            "embedding": emb,
            "llm_level1": l1,
            "llm_level2": l2,
            "llm_level3": l3,
        }
    return list(by_strong.values())


def find_nn(
    purpose: str,
    ref_corpus: list[dict[str, Any]],
    threshold: float,
) -> dict[str, str] | None:
    if not ref_corpus:
        return None
    emb = hashed_char_ngram_embedding(purpose)
    best: dict[str, Any] | None = None
    best_sim = -1.0
    for r in ref_corpus:
        sim = cosine_sim(emb, r["embedding"])
        if sim >= threshold and sim > best_sim:
            best_sim = sim
            best = r
    if best is None:
        return None
    return {
        "llm_level1": best["llm_level1"],
        "llm_level2": best["llm_level2"],
        "llm_level3": best["llm_level3"],
    }


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

    ref_path = env("EMBEDDING_REFERENCE_CSV", "").strip()
    nn_threshold = float(env("EMBED_NN_THRESHOLD", "0.92"))
    purpose_map: dict[str, dict[str, str]] = {}

    if ref_path:
        ref_file = Path(ref_path).resolve()
        if ref_file.exists():
            ref_corpus = load_reference_corpus(ref_file)
            print(f"Справочник эмбеддингов: {ref_file.name} | записей: {len(ref_corpus)} | порог NN: {nn_threshold}")
            resolved_by_nn = 0
            for p in unique_purposes:
                nn_labels = find_nn(p, ref_corpus, nn_threshold)
                if nn_labels:
                    purpose_map[p] = nn_labels
                    resolved_by_nn += 1
            unique_for_llm = [p for p in unique_purposes if p not in purpose_map]
            print(f"По векторной близости (NN) разрешено: {resolved_by_nn} | в LLM пойдёт: {len(unique_for_llm)}")
        else:
            print(f"Справочник не найден: {ref_file}")
            unique_for_llm = unique_purposes
    else:
        unique_for_llm = unique_purposes

    cfg = get_llm_cfg()
    print(f"LLM provider/model: {cfg.provider}/{cfg.model}")
    if unique_for_llm:
        llm_map = classify_unique_purposes(unique_for_llm, cfg)
        purpose_map.update(llm_map)
    for p in unique_purposes:
        if p not in purpose_map:
            purpose_map[p] = {"llm_level1": NEOPR, "llm_level2": NEOPR, "llm_level3": ""}

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
