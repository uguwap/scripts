"""
Главный скрипт для преобразования нечитаемых выписок в стандартный формат
через LLM API
"""
import sys
from pathlib import Path

from config import Config
from input_parser import read_unstructured_file
from llm_client import LLMClient
from excel_builder import build_standard_excel
from validator import validate_header, validate_all_operations


def process_statement(input_file: str, output_file: str, config: Config):
    """
    Основная функция обработки выписки
    
    Args:
        input_file: Путь к нечитаемой выписке
        output_file: Путь для сохранения стандартного Excel
        config: Конфигурация (API ключи, модель и т.д.)
    """
    
    print("=" * 70)
    print("🚀 LLM-парсер банковских выписок")
    print("=" * 70)
    print(f"📂 Входной файл: {input_file}")
    print(f"💾 Выходной файл: {output_file}")
    print(f"🤖 LLM провайдер: {config.llm_provider}")
    print(f"🧠 Модель: {config.model_name}")
    print("=" * 70)
    
    # ========== Шаг 1: Чтение файла ==========
    print("\n[1/4] 📖 Читаем и разбиваем файл на блоки...")
    try:
        file_data = read_unstructured_file(input_file, batch_size=config.batch_size)
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
        sys.exit(1)
    
    header_text = file_data["header"]
    operations_batches = file_data["operations_batches"]
    
    print(f"   ✅ Шапка: {len(header_text.split(chr(10)))} строк")
    print(f"   ✅ Операции разбиты на {len(operations_batches)} батч(ей)")
    
    # ========== Шаг 2: Инициализация LLM ==========
    print("\n[2/4] 🤖 Инициализируем LLM клиент...")
    try:
        llm = LLMClient(config)
    except Exception as e:
        print(f"❌ Ошибка инициализации LLM: {e}")
        sys.exit(1)
    print("   ✅ LLM клиент готов")
    
    # ========== Шаг 3: Извлечение шапки ==========
    print("\n[3/4] 🔍 Извлекаем шапку через LLM...")
    try:
        header = llm.extract_header(header_text)
    except Exception as e:
        print(f"❌ Ошибка извлечения шапки: {e}")
        sys.exit(1)
    
    # Валидация шапки
    is_valid, errors = validate_header(header)
    if not is_valid:
        print("   ❌ Ошибки валидации шапки:")
        for error in errors:
            print(f"      {error}")
        print("\n   ⚠️  Продолжаем с частично заполненной шапкой...")
    else:
        print("   ✅ Шапка извлечена и валидирована")
    
    print(f"\n   📋 Реквизиты должника:")
    print(f"      Название: {header.get('debtor_name', 'N/A')}")
    print(f"      ИНН: {header.get('debtor_inn', 'N/A')}")
    print(f"      Счёт: {header.get('debtor_account_number', 'N/A')}")
    print(f"      Банк: {header.get('debtor_bank_name', 'N/A')}")
    print(f"      Валюта: {header.get('currency_code', 'N/A')}")
    
    # ========== Шаг 4: Извлечение операций ==========
    print(f"\n[4/4] 🔍 Извлекаем операции ({len(operations_batches)} батчей)...")
    all_operations = []
    
    for batch_idx, batch_text in enumerate(operations_batches, start=1):
        print(f"   Обрабатываем батч {batch_idx}/{len(operations_batches)}...", end=" ")
        try:
            operations = llm.extract_operations(batch_text)
            print(f"✅ Получено {len(operations)} операций")
            all_operations.extend(operations)
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            continue
    
    print(f"\n   📊 Всего извлечено операций: {len(all_operations)}")
    
    # Валидация операций
    print("\n   🔍 Валидируем операции...")
    valid_operations, validation_errors = validate_all_operations(all_operations)
    
    if validation_errors:
        print(f"   ⚠️  Найдено {len(validation_errors)} ошибок валидации:")
        for error in validation_errors[:10]:  # Показываем первые 10
            print(f"      {error}")
        if len(validation_errors) > 10:
            print(f"      ... и ещё {len(validation_errors) - 10} ошибок")
    
    print(f"   ✅ Валидных операций: {len(valid_operations)} из {len(all_operations)}")
    
    if len(valid_operations) == 0:
        print("\n❌ Не удалось извлечь ни одной валидной операции!")
        sys.exit(1)
    
    # ========== Шаг 5: Генерация Excel ==========
    print("\n[5/5] 📝 Создаём стандартный Excel...")
    try:
        build_standard_excel(
            header=header,
            operations=valid_operations,
            output_path=output_file
        )
    except Exception as e:
        print(f"❌ Ошибка создания Excel: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("✅ ГОТОВО!")
    print("=" * 70)
    print(f"📄 Файл готов к обработке в bank-statement-normalizer:")
    print(f"   {output_file}")
    print("=" * 70)


def main():
    """Точка входа CLI"""
    if len(sys.argv) < 3:
        print("Использование:")
        print("  python main.py <входной_файл.xlsx> <выходной_файл.xlsx>")
        print("\nПример:")
        print("  python main.py nechitaemaya.xlsx standart.xlsx")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # Проверка существования входного файла
    if not Path(input_file).exists():
        print(f"❌ Файл не найден: {input_file}")
        sys.exit(1)
    
    # Загрузка конфигурации
    try:
        config = Config.from_env()
        config.validate()
    except ValueError as e:
        print(f"❌ Ошибка конфигурации: {e}")
        print("\nУстановите переменные окружения:")
        print("  LLM_PROVIDER=claude")
        print("  LLM_API_KEY=sk-ant-...")
        sys.exit(1)
    
    # Запуск обработки
    process_statement(input_file, output_file, config)


if __name__ == "__main__":
    main()

