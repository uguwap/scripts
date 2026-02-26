"""
Отладочный скрипт - показывает первые 30 строк RTF файла
"""
from striprtf.striprtf import rtf_to_text
import sys

def debug_rtf(file_path: str):
    print(f"\n{'='*80}")
    print(f"ОТЛАДКА RTF ФАЙЛА: {file_path}")
    print(f"{'='*80}\n")
    
    # Читаем RTF
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        rtf_content = f.read()
    
    # Конвертируем в текст
    text = rtf_to_text(rtf_content)
    
    # Разбиваем на строки
    lines = text.split('\n')
    
    print(f"📊 ВСЕГО СТРОК В RTF: {len(lines)}\n")
    print(f"{'='*80}")
    print("ПЕРВЫЕ 30 СТРОК RTF (то что видит LLM как шапка + начало операций):")
    print(f"{'='*80}\n")
    
    for idx, line in enumerate(lines[:30], start=1):
        line_clean = line.strip()
        if line_clean:
            print(f"Строка {idx:3d}: {line_clean}")
        else:
            print(f"Строка {idx:3d}: [ПУСТАЯ]")
    
    print(f"\n{'='*80}")
    print("АНАЛИЗ ШАПКИ (первые 20 строк):")
    print(f"{'='*80}\n")
    
    header_lines = []
    for idx, line in enumerate(lines[:20], start=1):
        clean = line.strip()
        if clean:
            header_lines.append(f"Строка {idx}: {clean}")
    
    header_text = "\n".join(header_lines)
    print(header_text)
    
    print(f"\n{'='*80}")
    print(f"ДЛИНА ТЕКСТА ШАПКИ: {len(header_text)} символов")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python debug_rtf_header.py <путь_к_rtf_файлу>")
        sys.exit(1)
    
    rtf_file = sys.argv[1]
    debug_rtf(rtf_file)



































