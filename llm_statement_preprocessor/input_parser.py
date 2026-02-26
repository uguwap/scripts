import pandas as pd
import re
from typing import Dict, List, Any
from pathlib import Path
from striprtf.striprtf import rtf_to_text

from logger import log_step


OPERATION_DATE_RE = re.compile(r"^\d{2}\.\d{2}\.\d{2,4}\b")
PREFIX_RE = re.compile(r"^Строка\s+(?P<line>\d+)\s*:\s*(?P<rest>.*)$")
TABLE_DATE_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{2,4}\b")


def parse_numbered_line(line: str) -> tuple[int | None, str]:
    m = PREFIX_RE.match(line.strip())
    if not m:
        return None, line.strip()
    try:
        return int(m.group("line")), (m.group("rest") or "").strip()
    except Exception:
        return None, (m.group("rest") or "").strip()


def looks_like_operation_start(line: str) -> bool:
    _, rest = parse_numbered_line(line)
    if OPERATION_DATE_RE.match(rest):
        return True

    if "│" in rest:
        cells = split_table_cells(rest)
        if len(cells) >= 2 and TABLE_DATE_RE.search(cells[1] or ""):
            first_cell_digits = re.sub(r"[^\d]", "", (cells[0] or "").strip())
            return bool(first_cell_digits)

    return False


def split_table_cells(text: str) -> list[str]:
    raw = [p.strip() for p in text.split("│")]
    if raw and raw[0] == "":
        raw = raw[1:]
    if raw and raw[-1] == "":
        raw = raw[:-1]
    return raw


def count_operations_in_text(text: str) -> int:
    return sum(1 for line in text.split("\n") if line.strip() and looks_like_operation_start(line))


def read_rtf_to_text(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        rtf_content = f.read()
    
    text = rtf_to_text(rtf_content)
    
    lines = text.split('\n')
    text_lines = []
    for idx, line in enumerate(lines, start=1):
        if line.strip():
            text_lines.append(f"Строка {idx}: {line.strip()}")
    
    log_step("RTF_PARSED", total_lines=len(text_lines))
    return "\n".join(text_lines)


def read_excel_to_text(file_path: str) -> str:
    df = pd.read_excel(file_path, header=None)
    df = df.fillna('')
    
    text_lines = []
    for idx, row in df.iterrows():
        row_text = " | ".join(str(cell).strip() for cell in row)
        text_lines.append(f"Строка {idx + 1}: {row_text}")
    
    log_step("EXCEL_PARSED", total_rows=len(df), total_lines=len(text_lines))
    return "\n".join(text_lines)


def split_header_and_operations(text_table: str, header_rows: int = 100) -> Dict[str, str]:
    lines = text_table.split('\n')

    split_idx = None
    for i, line in enumerate(lines):
        if looks_like_operation_start(line):
            split_idx = i
            break

    if split_idx is None:
        split_idx = min(header_rows, len(lines))

    header_lines = lines[:split_idx]
    operations_lines = lines[split_idx:]
    
    return {
        "header": "\n".join(header_lines),
        "operations": "\n".join(operations_lines)
    }


def group_lines_into_operations(operations_text: str) -> List[List[str]]:
    lines = [line for line in operations_text.split("\n") if line.strip()]
    groups: List[List[str]] = []
    current: List[str] = []

    for line in lines:
        if looks_like_operation_start(line):
            if current:
                groups.append(current)
            current = [line]
        else:
            if not current:
                continue
            current.append(line)

    if current:
        groups.append(current)

    return groups


def batch_operations(operations_text: str, batch_size: int = 50) -> List[str]:
    groups = group_lines_into_operations(operations_text)
    if not groups:
        return []

    batches: List[str] = []
    for i in range(0, len(groups), batch_size):
        batch_groups = groups[i : i + batch_size]
        flat_lines: List[str] = []
        for g in batch_groups:
            flat_lines.extend(g)
        batches.append("\n".join(flat_lines))
    return batches


def read_unstructured_file(file_path: str, batch_size: int = 50) -> Dict[str, Any]:
    file_path_obj = Path(file_path)
    
    if not file_path_obj.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    
    file_extension = file_path_obj.suffix.lower()
    log_step("FILE_READ_START", file=file_path, extension=file_extension)
    
    if file_extension == '.rtf':
        text_table = read_rtf_to_text(file_path)
    else:
        text_table = read_excel_to_text(file_path)
    
    log_step("TEXT_CONVERSION_DONE", total_chars=len(text_table))
    
    parts = split_header_and_operations(text_table)
    log_step("SPLIT_COMPLETE", header_chars=len(parts["header"]), operations_chars=len(parts["operations"]))
    
    operations_batches = batch_operations(parts["operations"], batch_size=batch_size)
    log_step("BATCHING_DONE", total_batches=len(operations_batches), batch_size=batch_size)
    
    return {
        "header": parts["header"],
        "operations_batches": operations_batches
    }
