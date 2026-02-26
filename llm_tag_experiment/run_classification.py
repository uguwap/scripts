"""
Эксперимент: LLM-классификация транзакций по трёхуровневой иерархии тегов.
Проект: БОРХИММАШ ФУЛЛ ПРО

Скрипт:
  1. Выгружает транзакции из ai_referent_merge (ClickHouse DEV)
  2. Обогащает ОКВЭД контрагентов из counterparties_info
  3. Батчами отправляет в LLM для классификации
  4. Сохраняет результат в CSV + сводку
"""

import csv
import json
import os
import sys
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# ──────────────────────────────────────────────────────────────────────
# Конфигурация
# ──────────────────────────────────────────────────────────────────────

# ClickHouse DEV
CH_HOST = "dev.ai-referent.ru"
CH_PORT = 8123
CH_USER = "i_litvinov"
CH_PASSWORD = "S7oSS2EauDQmWJ3CwpyF"
CH_DATABASE = "analytic"

# LLM
LLM_API_KEY = "Fp6BKzEAzCxLJUTgfV6T4BNyrOV6V9eM0nkSxTd9+rY="
LLM_BASE_URL = "https://neuro.sspb.ru/v1"
LLM_MODEL = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"

# Параметры батча
BATCH_SIZE = 40  # транзакций за один LLM-запрос
LLM_TIMEOUT = 180  # секунд
LLM_MAX_RETRIES = 3
SAMPLE_SIZE = 200  # 0 = все транзакции, >0 = случайная выборка для пилота

# Выходные файлы
OUTPUT_DIR = Path(__file__).parent / "output"

# ОКВЭДы лизинговых компаний
LEASING_OKVEDS = {"64.91", "64.91.1", "64.91.2", "77.11", "77.31", "77.33", "77.39"}


# ──────────────────────────────────────────────────────────────────────
# ClickHouse HTTP client
# ──────────────────────────────────────────────────────────────────────

def ch_query(sql: str, fmt: str = "JSONEachRow") -> list[dict]:
    """Выполняет запрос к ClickHouse через HTTP-интерфейс."""
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {
        "database": CH_DATABASE,
        "user": CH_USER,
        "password": CH_PASSWORD,
        "query": sql + f" FORMAT {fmt}",
    }
    resp = requests.get(url, params=params, timeout=60)
    if resp.status_code != 200:
        print(f"[CH ERROR] {resp.status_code}: {resp.text[:500]}")
        sys.exit(1)
    if not resp.text.strip():
        return []
    rows = []
    for line in resp.text.strip().split("\n"):
        if line.strip():
            rows.append(json.loads(line))
    return rows


def ch_query_text(sql: str) -> str:
    """Выполняет запрос, возвращает сырой текст."""
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {
        "database": CH_DATABASE,
        "user": CH_USER,
        "password": CH_PASSWORD,
        "query": sql,
    }
    resp = requests.get(url, params=params, timeout=60)
    if resp.status_code != 200:
        print(f"[CH ERROR] {resp.status_code}: {resp.text[:500]}")
        sys.exit(1)
    return resp.text.strip()


# ──────────────────────────────────────────────────────────────────────
# LLM client (OpenAI-compatible)
# ──────────────────────────────────────────────────────────────────────

def llm_classify(system_prompt: str, user_prompt: str, retry: int = 0) -> str:
    """Отправляет запрос к LLM и возвращает текст ответа."""
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 8192,
    }
    try:
        resp = requests.post(
            f"{LLM_BASE_URL}/chat/completions",
            headers=headers,
            json=payload,
            timeout=LLM_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"LLM HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        return content
    except Exception as e:
        if retry < LLM_MAX_RETRIES:
            wait = 2 ** (retry + 1)
            print(f"  [LLM] Ошибка ({e}), повтор через {wait}с...")
            time.sleep(wait)
            return llm_classify(system_prompt, user_prompt, retry + 1)
        raise


# ──────────────────────────────────────────────────────────────────────
# Промпт классификатора
# ──────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Ты — финансовый аналитик. Твоя задача — классифицировать банковские транзакции по трёхуровневой иерархии тегов.

## Правила:
1. Каждая транзакция получает РОВНО ОДИН тег на каждом уровне.
2. Если debit_amount > 0 — это СПИСАНИЕ (дебет). Если credit_amount > 0 — это ПОСТУПЛЕНИЕ (кредит).
3. Используй назначение платежа (payment_purpose) как ГЛАВНЫЙ источник информации.
4. Если контрагент отмечен как is_leasing_company=true — платежи по договорам с ним, вероятно, лизинговые.
5. Верни СТРОГО JSON-массив, без комментариев, без markdown-разметки.

## КЛАССИФИКАТОР — ПОСТУПЛЕНИЯ (credit_amount > 0):

Уровень 1: "Поступления"
Уровень 2 → Уровень 3:
- "От заказчиков/покупателей"
  - "Транспортные услуги / транспортная экспедиция"
  - "Оплата за товары"
  - "Прочие услуги"
- "Поступления от руководителей/учредителей"
  - (нет подуровня — пустая строка)
- "Перевод собственных средств"
  - (нет подуровня)
- "Внесение наличных средств"
  - (нет подуровня)
- "Поступления от третьих лиц"
  - (нет подуровня)
- "Поступления займов/кредитов/финансирование"
  - "Займы от учредителей"
  - "Займы от юрлиц"
  - "Банковские кредиты"
  - "Факторинг и другие виды финансирования"
- "Возвраты"
  - "Подотчётных средств"
  - "Ошибочных платежей"
- "Иные поступления"
  - (нет подуровня)

## КЛАССИФИКАТОР — СПИСАНИЯ (debit_amount > 0):

Уровень 1: "Списания"
Уровень 2 → Уровень 3:
- "Платежи контрагентам"
  - "Транспортные услуги / транспортная экспедиция"
  - "Платон (ООО РТИС)"
  - "Топливо, ГСМ, нефтепродукты, запчасти"
  - "Аренда базы, офиса"
  - "Ремонт, ТО/техобслуживание"
  - "Оплата за товары"
  - "Прочие контрагенты"
- "Лизинговые платежи"
  - (нет подуровня; включая пени; контрагент — лизинговая компания по ОКВЭД)
- "Погашение кредита"
  - (включая процентные платежи)
- "Погашение займа"
  - (включая процентные платежи)
- "Погашение по договорам коммерческого финансирования"
  - (включая комиссии, проценты)
- "Займы выданные"
  - (нет подуровня)
- "Снятие наличных / корпоративные карты"
  - "Выдача наличных"
- "Банковские комиссии"
  - (нет подуровня)
- "Отчисления, налоги"
  - (нет подуровня)
- "Переводы собственных средств"
  - (нет подуровня)
- "Переводы аффилированным лицам (L1)"
  - (руководители/учредители компании/ИП)
- "Иные списания"
  - (нет подуровня)

## Формат ответа

Верни JSON-массив. Каждый элемент:
{
  "row_id": <номер строки из входных данных>,
  "level_1": "Поступления" или "Списания",
  "level_2": "<тег 2-го уровня>",
  "level_3": "<тег 3-го уровня или пустая строка>"
}

ВАЖНО: Верни ТОЛЬКО JSON-массив. Без ```json, без пояснений, без think-блоков."""


# ──────────────────────────────────────────────────────────────────────
# Шаг 1. Обнаружение проекта
# ──────────────────────────────────────────────────────────────────────

def discover_project() -> tuple[str, str]:
    """Возвращает project_id и project_name для БОРХИММАШ ФУЛЛ ПРОЕКТ."""
    print("=" * 60)
    print("Шаг 1. Проект БОРХИММАШ ФУЛЛ ПРОЕКТ")
    # Захардкожено по результатам discover
    pid = "1b64607b-b39e-4cbe-b94b-ec10f88fe9fc"
    pname = "БОРХИММАШ ФУЛЛ ПРОЕКТ"
    # Проверим количество
    rows = ch_query(f"""
        SELECT count() AS cnt
        FROM ai_referent_merge
        WHERE project_id = '{pid}' AND project_name = '{pname}'
    """)
    cnt = int(rows[0]["cnt"]) if rows else 0
    print(f"  project_id={pid}")
    print(f"  project_name={pname}")
    print(f"  Транзакций: {cnt}")
    return pid, pname


# ──────────────────────────────────────────────────────────────────────
# Шаг 2. Выгрузка транзакций
# ──────────────────────────────────────────────────────────────────────

def extract_transactions(project_id: str) -> list[dict]:
    """Выгружает транзакции из ai_referent_merge + ОКВЭД контрагентов."""
    print("\nШаг 2. Выгрузка транзакций...")

    limit_clause = f"LIMIT {SAMPLE_SIZE}" if SAMPLE_SIZE > 0 else ""
    order_clause = "ORDER BY rand()" if SAMPLE_SIZE > 0 else "ORDER BY arm.document_operation_date, arm.document_number"

    rows = ch_query(f"""
        SELECT
            arm.document_number,
            arm.document_operation_date,
            arm.payer_or_recipient_name,
            arm.payer_or_recipient_inn,
            arm.debtor_inn,
            arm.debtor_name,
            arm.debit_amount,
            arm.credit_amount,
            arm.payment_purpose,
            arm.tag,
            arm.debtor_account_number,
            arm.account_number,
            coalesce(
                nullIf(
                    arrayFirst(
                        x -> x != '',
                        arrayMap(
                            (c, m) -> if(m = 1 AND c != '', c, ''),
                            ci.`activity_types.code`,
                            ci.`activity_types.is_main`
                        )
                    ), ''
                ),
                ''
            ) AS counterparty_okved,
            arrayStringConcat(ci.`activity_types.code`, ',') AS counterparty_all_okveds,
            coalesce(
                nullIf(arrayFirst(
                    x -> x != '',
                    arrayMap(
                        (n, a) -> if(a = 1 AND n != '', n, ''),
                        ci.`names.short_name`,
                        ci.`names.is_actual`
                    )
                ), ''),
                arm.payer_or_recipient_name
            ) AS counterparty_name_std
        FROM ai_referent_merge AS arm
        LEFT JOIN counterparties_info AS ci
            ON ci.inn = arm.payer_or_recipient_inn
        WHERE arm.project_id = '{project_id}'
          AND arm.project_name = 'БОРХИММАШ ФУЛЛ ПРОЕКТ'
        {order_clause}
        {limit_clause}
    """)

    print(f"  Загружено {len(rows)} транзакций")

    # Обогащение: пометка лизинговых компаний (по основному ИЛИ любому дополнительному ОКВЭД)
    for row in rows:
        main_okved = row.get("counterparty_okved", "")
        all_okveds_str = row.get("counterparty_all_okveds", "")
        all_okveds = [o.strip() for o in all_okveds_str.split(",") if o.strip()] if all_okveds_str else []
        if main_okved and main_okved not in all_okveds:
            all_okveds.append(main_okved)

        row["is_leasing_company"] = any(
            any(okved == code or okved.startswith(code + ".") for code in LEASING_OKVEDS)
            for okved in all_okveds
        ) if all_okveds else False

    # Статистика
    debit_count = sum(1 for r in rows if float(r.get("debit_amount", 0)) > 0)
    credit_count = sum(1 for r in rows if float(r.get("credit_amount", 0)) > 0)
    leasing_count = sum(1 for r in rows if r["is_leasing_company"])
    print(f"  Дебет: {debit_count}, Кредит: {credit_count}, Лизинг-контрагенты: {leasing_count}")

    return rows


# ──────────────────────────────────────────────────────────────────────
# Шаг 3. LLM-классификация батчами
# ──────────────────────────────────────────────────────────────────────

def format_batch_for_llm(rows: list[dict], start_idx: int) -> str:
    """Форматирует батч транзакций для промпта."""
    lines = []
    for i, row in enumerate(rows):
        row_id = start_idx + i
        debit = float(row.get("debit_amount", 0))
        credit = float(row.get("credit_amount", 0))
        direction = "СПИСАНИЕ" if debit > 0 else "ПОСТУПЛЕНИЕ"
        amount = debit if debit > 0 else credit
        leasing_flag = "true" if row.get("is_leasing_company") else "false"

        lines.append(
            f"[{row_id}] {direction} | {amount:.2f} руб. | "
            f"Контрагент: {row.get('payer_or_recipient_name', '?')} "
            f"(ИНН: {row.get('payer_or_recipient_inn', '?')}) | "
            f"is_leasing_company={leasing_flag} | "
            f"Назначение: {row.get('payment_purpose', '?')}"
        )
    return "\n".join(lines)


def parse_llm_response(text: str) -> list[dict]:
    """Извлекает JSON-массив из ответа LLM (возможно с think-блоком)."""
    # Убираем <think>...</think> если есть
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    # Убираем markdown code fences
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    # Ищем JSON-массив
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Пробуем распарсить целиком
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    print(f"  [WARN] Не удалось распарсить ответ LLM ({len(text)} символов)")
    print(f"  Начало: {text[:200]}")
    return []


def classify_transactions(rows: list[dict]) -> list[dict]:
    """Классифицирует транзакции через LLM батчами."""
    print(f"\nШаг 3. LLM-классификация ({len(rows)} транзакций, батч={BATCH_SIZE})...")

    all_tags: dict[int, dict] = {}
    total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(rows))
        batch = rows[start:end]

        print(f"  Батч {batch_idx + 1}/{total_batches} (строки {start}–{end - 1})...", end=" ", flush=True)

        user_prompt = (
            "Классифицируй следующие транзакции по трёхуровневому классификатору.\n\n"
            + format_batch_for_llm(batch, start)
        )

        t0 = time.time()
        response_text = llm_classify(SYSTEM_PROMPT, user_prompt)
        elapsed = time.time() - t0

        parsed = parse_llm_response(response_text)
        print(f"OK ({len(parsed)} тегов, {elapsed:.1f}с)")

        for item in parsed:
            rid = item.get("row_id")
            if rid is not None:
                all_tags[int(rid)] = {
                    "level_1": item.get("level_1", ""),
                    "level_2": item.get("level_2", ""),
                    "level_3": item.get("level_3", ""),
                }

        # Пауза между батчами
        if batch_idx < total_batches - 1:
            time.sleep(1)

    # Объединяем
    for i, row in enumerate(rows):
        tag = all_tags.get(i, {})
        row["llm_level_1"] = tag.get("level_1", "НЕ ОПРЕДЕЛЁН")
        row["llm_level_2"] = tag.get("level_2", "НЕ ОПРЕДЕЛЁН")
        row["llm_level_3"] = tag.get("level_3", "")

    classified = sum(1 for r in rows if r["llm_level_1"] != "НЕ ОПРЕДЕЛЁН")
    print(f"  Классифицировано: {classified}/{len(rows)}")

    return rows


# ──────────────────────────────────────────────────────────────────────
# Шаг 4. Сохранение результатов
# ──────────────────────────────────────────────────────────────────────

def save_results(rows: list[dict], project_name: str):
    """Сохраняет CSV и сводную статистику."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # CSV
    csv_path = OUTPUT_DIR / f"classification_{ts}.csv"
    fieldnames = [
        "document_number", "document_operation_date",
        "payer_or_recipient_name", "payer_or_recipient_inn",
        "counterparty_okved", "is_leasing_company",
        "debtor_name", "debtor_inn",
        "debit_amount", "credit_amount",
        "payment_purpose", "tag",
        "llm_level_1", "llm_level_2", "llm_level_3",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"\n  CSV сохранён: {csv_path}")

    # Сводка
    summary_path = OUTPUT_DIR / f"summary_{ts}.txt"
    stats_l2: dict[str, int] = {}
    stats_l2_amount: dict[str, float] = {}
    for row in rows:
        key = f"{row.get('llm_level_1', '?')} → {row.get('llm_level_2', '?')}"
        stats_l2[key] = stats_l2.get(key, 0) + 1
        debit = float(row.get("debit_amount", 0))
        credit = float(row.get("credit_amount", 0))
        stats_l2_amount[key] = stats_l2_amount.get(key, 0.0) + max(debit, credit)

    lines = [
        f"Проект: {project_name}",
        f"Всего транзакций: {len(rows)}",
        f"Дата выгрузки: {datetime.now().isoformat()}",
        f"Модель: {LLM_MODEL}",
        "",
        "=" * 80,
        "СВОДКА ПО ТЕГАМ (Уровень 1 → Уровень 2)",
        "=" * 80,
        "",
        f"{'Тег':<60} {'Кол-во':>7} {'Сумма, руб':>15}",
        "-" * 85,
    ]
    for key in sorted(stats_l2.keys()):
        cnt = stats_l2[key]
        amt = stats_l2_amount[key]
        lines.append(f"{key:<60} {cnt:>7} {amt:>15,.2f}")

    lines.extend(["", "=" * 80, "ДЕТАЛИ ПО УРОВНЮ 3", "=" * 80, ""])
    stats_l3: dict[str, int] = {}
    for row in rows:
        key = f"{row.get('llm_level_1', '?')} → {row.get('llm_level_2', '?')} → {row.get('llm_level_3', '')}"
        stats_l3[key] = stats_l3.get(key, 0) + 1
    for key in sorted(stats_l3.keys()):
        lines.append(f"  {key:<75} {stats_l3[key]:>5}")

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  Сводка сохранена: {summary_path}")

    # Печать сводки в консоль
    print("\n" + "\n".join(lines))


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  LLM Tag Classification Experiment                     ║")
    print("║  Модель: DeepSeek-R1-Distill-Qwen-32B                  ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # 1. Найти проект
    project_id, project_name = discover_project()

    # 2. Выгрузить транзакции
    transactions = extract_transactions(project_id)
    if not transactions:
        print("Нет транзакций — завершение.")
        return

    # 3. Классифицировать через LLM
    transactions = classify_transactions(transactions)

    # 4. Сохранить результаты
    save_results(transactions, project_name)

    print("\n✅ Эксперимент завершён!")


if __name__ == "__main__":
    main()

