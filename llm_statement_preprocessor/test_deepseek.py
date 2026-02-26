"""
Простой тест DeepSeek API для проверки что всё работает
"""
import os
from openai import OpenAI

def test_deepseek():
    """Быстрый тест подключения к DeepSeek API"""
    
    # Проверка наличия API ключа
    api_key = os.getenv("LLM_API_KEY")
    if not api_key:
        print("❌ Ошибка: Не установлена переменная LLM_API_KEY")
        print("\nУстановите её командой:")
        print("  set LLM_API_KEY=sk-ваш-ключ")
        return False
    
    print("=" * 60)
    print("🧪 Тест DeepSeek API")
    print("=" * 60)
    print(f"🔑 API ключ: {api_key[:10]}...{api_key[-4:]}")
    
    try:
        # Создаём клиент
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com"
        )
        
        print("\n📤 Отправляем тестовый запрос...")
        
        # Простой тестовый запрос
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": "Ты помощник для работы с банковскими выписками."
                },
                {
                    "role": "user",
                    "content": "Извлеки ИНН из этого текста: 'ООО Ромашка, ИНН 7701234567'. Верни только JSON: {\"inn\": \"...\"}"
                }
            ],
            temperature=0,
            max_tokens=100
        )
        
        result = response.choices[0].message.content
        
        print("\n📥 Получен ответ от DeepSeek:")
        print(result)
        
        # Проверяем что в ответе есть ИНН
        if "7701234567" in result:
            print("\n✅ Тест ПРОЙДЕН!")
            print("DeepSeek API работает корректно.")
            print("\nМожете запускать основной скрипт:")
            print("  python main.py input.xlsx output.xlsx")
            return True
        else:
            print("\n⚠️  Тест прошёл, но ответ неожиданный")
            print("DeepSeek API работает, но возможно нужно улучшить промпт")
            return True
            
    except Exception as e:
        print(f"\n❌ Ошибка подключения к DeepSeek API:")
        print(f"   {e}")
        print("\nПроверьте:")
        print("  1. API ключ правильный (https://platform.deepseek.com/api_keys)")
        print("  2. Есть интернет подключение")
        print("  3. Установлена библиотека: pip install openai")
        return False


if __name__ == "__main__":
    # Для запуска:
    # set LLM_API_KEY=sk-ваш-ключ
    # python test_deepseek.py
    
    test_deepseek()

