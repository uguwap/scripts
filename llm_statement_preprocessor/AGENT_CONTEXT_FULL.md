## Контекст проекта (для другого ИИ‑агента)

### Цель
Нужно обрабатывать нечитаемые банковские выписки (RTF/Excel/CSV), извлекать шапку и операции через LLM (в основном DeepSeek), формировать Excel в формате, который корректно парсится легаси‑сервисом `normalizers/bank-statement-normalizer`.

Критичное бизнес‑требование: **не терять данные** (суммы/ИНН/назначение/кавычки/символы) и обеспечить прозрачность (аудит каждого батча).

### Архитектура текущего решения
Путь: `scripts/llm_statement_preprocessor`

Компоненты:
- `api.py` — FastAPI сервис (Swagger `/docs`), эндпоинт `/preprocess/statement`.
- `input_parser.py` — чтение RTF/Excel → строковый “псевдо‑табличный” вид + разбиение на header/operations + батчи.
- `prompts.py` — системные промпты LLM для header и operations.
- `llm_client.py` — вызов LLM, ретраи, repair‑prompt на повторных попытках, парсинг JSON.
- `data_normalizer.py` — нормализация значений под правила/ожидания нормалайзера без потери исходной информации.
- `excel_builder.py` — генерация Excel (header + таблица).
- `services.py` — оркестрация пайплайна + checkpointing + логирование “failed batches”.
- `run_logs.py` — структурированные артефакты логов по каждому запуску (для переиспользования/ручного восстановления).

Артефакты:
- `uploads/` — все Excel файлы (checkpoint_* и final_all_operations.xlsx)
- `logs/` — логи запусков (run_YYYYMMDD_HHMMSS/...)

### Контракт входного Excel для bank-statement-normalizer
Нормалайзер парсит Excel и:
- читает xlsx целиком (важно: не read_only) и далее через pandas `header=None`.
- находит блок таблицы операций по эвристикам заполненности строк и regex токенам заголовков.
- маппит заголовки таблицы по regex (см. `normalizers/bank-statement-normalizer/app/constants/regex_patterns.py`).
- общую инфу (debtor_account_number/debtor_name/debtor_inn/...) ищет в верхнем блоке тоже по regex.

Поэтому:
- `excel_builder.py` делает шапку с ключевыми словами, которые реально матчатся regex-ами:
  - `выписка по счету` (счёт должника)
  - `клиент:` (должник)
  - `инн` (ИНН должника)
  - `код валюты` (валюта)
  - `наименование банка кредитных организаций` (банк должника)
- таблица имеет 2 строки заголовков (верхний уровень блоков + нижний уровень колонок) и далее строки операций.

### Логика ретраев и “невалидного JSON”
Проблема: DeepSeek иногда возвращает синтаксически невалидный JSON (пропуски кавычек, сырые переносы строк и т.п.).

Решение:
- Первая попытка: базовый промпт.
- Повторные попытки: **repair‑prompt**, в котором:
  - передаётся ошибка парсинга,
  - исходный текст батча,
  - предыдущий ответ модели,
  - строгие правила: “не менять данные, только починить JSON; экранировать кавычки и переносы”.

Парсинг:
- `parse_json_safely` вырезает JSON‑подстроку и делает `json.loads` без “удаления символов” (чтобы не терять данные).

### Нормализация данных (без потери)
`data_normalizer.py`:
- `normalize_amount` поддерживает формат `12345-67` → `12345.67` (как в нормалайзере `convert_to_float`) и не превращает суммы в “другие числа”.
- текстовые поля не “чистятся” от кавычек; только минимально нормализуются пробелы.
- ИНН/счета/БИК/КПП: пытаемся привести к цифрам, но если не получается — возвращаем исходное значение (не выкидываем).

### Checkpoints и финальный Excel
В процессе обработки:
- каждые `checkpoint_interval` батчей (обычно 50) создаётся `checkpoint_{start}-{end}.xlsx` в `uploads/`.
- в конце создаётся `final_all_operations.xlsx` в `uploads/` всегда.

Текущая реализация чекпоинтов — кумулятивная: в checkpoint попадает всё `all_operations` на текущий момент.

### Прозрачность / аудит / “ничего не теряем”
Для каждого запуска создаётся `logs/run_.../`:
- `source_header.txt`, `source_operations.txt` — полные тексты, которые пошли на LLM (для восстановления).
- `header/attempt_XX/{prompt.txt,response.txt,error.txt,event.json}`
- `batches/batch_NNNN/input.txt`
- `batches/batch_NNNN/attempt_XX/{prompt.txt,response.txt,error.txt,event.json}`
- `summary.json` — список failed_batches и метаданные
- `index.json` — навигационный индекс (какой батч/попытка где лежит)

### Как запускать
1) Активировать venv и поднять API:
- `cd scripts/llm_statement_preprocessor`
- `.\venv\Scripts\python.exe -m uvicorn api:app --host 0.0.0.0 --port 9000 --reload`

2) Swagger: `/docs`
3) POST `/preprocess/statement`:
- file: RTF/Excel/CSV
- enable_checkpoints: true
- checkpoint_interval: 50
- strict_mode: true/false (если true — при провалах батчей будет статус ошибки; файлы и логи всё равно сохраняются)

### Известные нюансы
1) В `normalizers/bank-statement-normalizer/app/config/result_table_config.json` есть подозрение на перепутанные `data_variable` для “Код валюты” и “Должник” (это конфиг нормалайзера, не этого сервиса). Мы не правим нормалайзер, мы подстраиваемся под его фактический парсинг по regex.
2) Linter warnings про `openai/anthropic/groq/google.generativeai` — зависят от окружения и не блокируют запуск, если провайдер не используется.




































