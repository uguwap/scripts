# Контекст для AI-агента: эксперимент LLM-тегов по выпискам (лизинг)

## Задача

По проекту с лизингом провести эксперимент на данных выписок: извлечь транзакции из PDF, пройти каждую по формату типизации платежей (level1 / level2 / level3) через LLM и сохранить результат.

## Что сделано

### 1. Экстракция транзакций из PDF

- **Скрипт:** `extract_transactions_from_pdfs.py`
- **Вход:** PDF-файлы в `scripts/llm_tag_experiment/files/*.pdf`
- **Логика:** pdfplumber извлекает текст, по строкам ищет дату (dd.mm.yyyy или dd.mm.yy), два денежных поля (дебет/кредит), контрагента (после 20-значного счёта) и назначение платежа (хвост строки + последующие строки до следующей даты). Даты вида dd.mm.yy нормализуются в dd.mm.yyyy.
- **Выход:** один CSV в `output/` с колонками: `document_operation_date`, `debit_amount`, `credit_amount`, `payer_or_recipient_name`, `payment_purpose`, `source_file`.
- **Статус:** работает. Уже получен файл `output/extracted_transactions_20260310_114243.csv` (937 строк по двум PDF: ВТБ_ИП.pdf, ВТБ_ЮЛ.pdf).

### 2. LLM-классификация назначений платежей

- **Скрипт:** `classify_payment_purposes_llm.py`
- **Вход:** CSV из шага 1 (по умолчанию последний `output/extracted_transactions_*.csv`; можно задать через `EXTRACTED_CSV_PATH`).
- **Логика:** собираются уникальные значения `payment_purpose`, разбиваются на батчи (размер из `LLM_BATCH_SIZE`, по умолчанию 25). Для каждого батча отправляется запрос к OpenAI-совместимому API (см. переменные ниже), ответ парсится как JSON-массив с полями `id`, `llm_level1`, `llm_level2`, `llm_level3`. Допустимые значения level1: Incoming, Outgoing, НЕ ОПРЕДЕЛЕНО; level2/level3 — из фиксированных словарей в коде. Невалидные ответы приводятся к «НЕ ОПРЕДЕЛЕНО». По `payment_purpose` строится маппинг тегов и применяется ко всем строкам исходного CSV.
- **Выход:** в `output/` пишутся:
  - `classification_from_files_YYYYMMDD_HHMMSS.xlsx` — для загрузки в Postgres (формат как у load_results_to_postgres);
  - `result_adaptive_from_files_YYYYMMDD_HHMMSS.csv` — для convert_to_excel.py (отчёт с листами Транзакции / Сводка / SQL vs LLM).
- **Переменные окружения:** `LLM_API_KEY`, `LLM_BASE_URL` (например `https://neuro.sspb.ru/v1`), `LLM_MODEL`, `LLM_PROVIDER`, `LLM_MAX_TOKENS`, `LLM_TEMPERATURE`. Опционально `.env` в папке скрипта или в корне ai-referent.

### 3. Обёртка запуска классификатора

- **Скрипт:** `run_classify.ps1` (PowerShell)
- **Назначение:** выставляет переменные окружения (в т.ч. креды и путь к CSV), вызывает `python .\classify_payment_purposes_llm.py`. Путь к CSV: сначала `output/extracted_transactions_20260310_114243.csv`, при отсутствии — последний по времени `output/extracted_transactions_*.csv`.
- **Запуск:** из папки `scripts/llm_tag_experiment` выполнить: `.\run_classify.ps1`

### 4. Прочее

- В корне `scripts/` добавлен `.gitignore` (venv, __pycache__, .pytest_cache и т.п.), чтобы не коммитить виртуальные окружения.
- В `classify_payment_purposes_llm.py` добавлена поддержка `LLM_BASE_URL` для кастомного OpenAI-совместимого endpoint.

## Проблема запуска (для отладки)

При запуске **из агента Cursor** команды, которая выполняет `classify_payment_purposes_llm.py` (напрямую или через `run_classify.ps1`), процесс не доходит до конца: возвращается **`Command failed to spawn: Aborted`**. Короткие команды в том же терминале (например `python --version`, смена каталога, установка env) выполняются нормально.

**Предположения (не подтверждены на стороне агента):**

- Ограничение среды Cursor на длительные или сетевые процессы, запускаемые агентом.
- Возможное влияние Windows Defender или другого AV (блокировка процесса/сети) — проверять на машине пользователя.

**Что делать:** классификатор нужно запускать **вручную в терминале пользователя** (вне агента):  
`cd scripts\llm_tag_experiment` → `.\run_classify.ps1`. После успешного прогона в `output/` появятся XLSX и CSV с тегами; дальше при необходимости: `load_results_to_postgres.py` по XLSX или `convert_to_excel.py` по CSV.

## 5. Эмбеддинги и векторная близость (меньше запросов, больше покрытия)

**Идея:** по векторной близости назначений подставлять теги от уже размеченного соседа вместо вызова LLM — меньше запросов, те же кейсы покрываются.

### 5.1 Классификатор: режим «сначала NN по справочнику»

- **Переменные:** `EMBEDDING_REFERENCE_CSV` — путь к CSV-справочнику (например предыдущий `result_adaptive_from_files_*.csv`); `EMBED_NN_THRESHOLD` — порог косинусной близости (по умолчанию 0.92).
- **Логика:** для каждого уникального `payment_purpose` считаются локальные эмбеддинги (hashed char 3–5-граммы, как в load_results). В справочнике ищется ближайший сосед; если sim ≥ порог — тег берётся из справочника, в LLM не отправляется. В LLM идут только назначения без близкого совпадения.
- **Запуск:** `$env:EMBEDDING_REFERENCE_CSV = "output\result_adaptive_from_files_20260311_114334.csv"; .\run_classify.ps1`

### 5.2 Postgres + pgvector (корпус, NN, обогащённый отчёт)

- **load_results_to_postgres.py** — загружает XLSX с LLM-тегами в схему `experiments`: сырые строки в `tx_classification_raw`, корпус уникальных назначений с эмбеддингами в `purpose_corpus`, теги в `purpose_labels`. Эмбеддинг — тот же hashed char n-gram (не семантический API).
- **pgvector_experiment.py** — пересчитывает таблицу `purpose_nn` (ближайший сосед по эмбеддингу), считает LOOCV по размеченным, экспортирует `output/classification_partial_pgvector.xlsx`: колонки `sim`, `nn_level_1/2/3`, `resolved_source` (llm | pgvector_auto_ge_0_90 | pgvector_review_0_80_0_90 | legacy_only | unresolved).
- **Запуск пайплайна:** `.\run_pgvector.ps1` (берёт последний `classification_from_files_*.xlsx` из `output/`) или `.\run_pgvector.ps1 -XlsxPath "output\classification_from_files_20260311_114334.xlsx"`. Требуется Postgres с расширением pgvector.

## Дальнейшие шаги (если продолжит другой агент)

1. Убедиться, что в `output/` есть актуальный `extracted_transactions_*.csv`.
2. Запустить классификацию (локально у пользователя): `.\run_classify.ps1` или с эмбеддинг-справочником — задать `EMBEDDING_REFERENCE_CSV` и при необходимости `EMBED_NN_THRESHOLD`.
3. При необходимости — загрузка в Postgres и pgvector: `.\run_pgvector.ps1`; или только отчёт: `python convert_to_excel.py output\result_adaptive_from_files_....csv`.
4. Если нужно разобрать «Aborted» при запуске из агента — проверить таймауты, песочницу и логи Cursor; на стороне пользователя — исключения Defender/AV для папки проекта или python.exe.
