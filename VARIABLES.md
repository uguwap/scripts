# 📋 СПИСОК ВСЕХ ПЕРЕМЕННЫХ ДЛЯ ПОДКЛЮЧЕНИЯ К БД

## 🔴 ПРОДАКШН (PROD) - ClickHouse через SSH туннель

**Используется в:** `egrul_report.py`

| Переменная | Описание | Пример значения |
|------------|----------|-----------------|
| `SSH_HOST` | Хост SSH сервера для туннеля | `doc.ai-referent.ru` |
| `SSH_PORT` | Порт SSH | `22` |
| `SSH_USER` | Пользователь SSH | `tunnel` |
| `SSH_PASSWORD` | Пароль SSH | `your_ssh_password_here` |
| `CH_REMOTE_HOST` | Внутренний хост ClickHouse (за SSH) | `10.10.0.4` |
| `CH_REMOTE_PORT` | Порт ClickHouse | `9000` |
| `CH_USER` | Пользователь ClickHouse | `i_litvinov` |
| `CH_PASSWORD` | Пароль ClickHouse PROD | `your_ch_prod_password_here` |
| `CH_DATABASE` | Имя базы данных | `analytic` |

**Схема подключения:**
```
Локальный ПК → SSH туннель (doc.ai-referent.ru:22) → ClickHouse (10.10.0.4:9000)
```

---

## 🟢 РАЗРАБОТКА (DEV) - ClickHouse прямое подключение

**Используется в:** `llm_tag_experiment/run_classification.py`

| Переменная | Описание | Пример значения |
|------------|----------|-----------------|
| `CH_DEV_HOST` | Хост ClickHouse DEV | `dev.ai-referent.ru` |
| `CH_DEV_PORT` | Порт ClickHouse HTTP | `8123` |
| `CH_DEV_USER` | Пользователь ClickHouse | `i_litvinov` |
| `CH_DEV_PASSWORD` | Пароль ClickHouse DEV | `your_ch_dev_password_here` |
| `CH_DEV_DATABASE` | Имя базы данных | `analytic` |

**Схема подключения:**
```
Локальный ПК → HTTP → ClickHouse DEV (dev.ai-referent.ru:8123)
```

---

## 🤖 LLM API ПЕРЕМЕННЫЕ

### Для `llm_tag_experiment/run_classification.py`

| Переменная | Описание | Пример значения |
|------------|----------|-----------------|
| `LLM_API_KEY` | API ключ для LLM сервиса | `your_llm_api_key_here` |
| `LLM_BASE_URL` | Базовый URL API | `https://neuro.sspb.ru/v1` |
| `LLM_MODEL` | Название модели | `deepseek-ai/DeepSeek-R1-Distill-Qwen-32B` |

### Для `llm_statement_preprocessor/` (через config.py)

| Переменная | Описание | Значение по умолчанию |
|------------|----------|----------------------|
| `LLM_PROVIDER` | Провайдер LLM | `claude` (claude, openai, gemini, deepseek, groq) |
| `LLM_API_KEY` | API ключ (общий) | - |
| `LLM_MODEL` | Модель (зависит от провайдера) | `claude-3-5-sonnet-20241022` |
| `LLM_MAX_TOKENS` | Максимум токенов | `4096` |
| `LLM_TEMPERATURE` | Температура (0.0 = детерминированный) | `0.0` |
| `LLM_ENABLED` | Включить/выключить LLM | `1` (1/0) |
| `BATCH_SIZE` | Размер батча | `10` |

---

## ⚙️ ПАРАМЕТРЫ СКРИПТОВ

### egrul_report.py

| Переменная | Описание | Значение по умолчанию |
|------------|----------|----------------------|
| `DAYS_BACK` | Количество дней для анализа | `30` |

### llm_tag_experiment/run_classification.py

| Переменная | Описание | Значение по умолчанию |
|------------|----------|----------------------|
| `BATCH_SIZE` | Транзакций за один LLM-запрос | `40` |
| `LLM_TIMEOUT` | Таймаут запросов к LLM (секунды) | `180` |
| `LLM_MAX_RETRIES` | Максимум повторов при ошибке | `3` |
| `SAMPLE_SIZE` | Размер выборки (0 = все данные) | `200` |

---

## 📝 ПРИМЕЧАНИЯ

1. **Все пароли и API ключи должны храниться в `.env` файле**
2. Файл `.env` не коммитится в репозиторий (добавлен в `.gitignore`)
3. Используйте `.env.example` как шаблон для создания своего `.env`
4. При утечке паролей - немедленно смените их в соответствующих системах

---

## 🔄 МИГРАЦИЯ С ХАРДКОДА

Если у вас уже есть скрипты с захардкоженными значениями:

1. Скопируйте `.env.example` в `.env`
2. Заполните все переменные реальными значениями
3. Скрипты автоматически подхватят значения из `.env`

**Важно:** Все реальные пароли и ключи должны храниться только в вашем локальном файле `.env`, который не коммитится в репозиторий.

