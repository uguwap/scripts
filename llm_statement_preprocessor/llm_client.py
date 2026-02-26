import json
import re
import time
from typing import Any, Dict, List, Optional
from openai import OpenAI
from anthropic import Anthropic

from config import Config
from prompts import HEADER_SYSTEM_PROMPT, OPERATIONS_SYSTEM_PROMPT
from logger import log_step, log_llm_request, log_llm_response, log_error
from data_normalizer import normalize_header, normalize_operations
from run_logs import RunLogs


def retry_sleep_seconds(attempt: int, cap: int = 10) -> int:
    return min(2 ** attempt, cap)


class LLMClient:
    def __init__(self, config: Config, run_logs: Optional[RunLogs] = None):
        self.config = config
        self.provider = config.llm_provider
        self.run_logs = run_logs
        
        if self.provider == "claude":
            self.client = Anthropic(api_key=config.api_key)
        
        elif self.provider in ["openai", "deepseek"]:
            base_url = "https://api.deepseek.com" if self.provider == "deepseek" else None
            self.client = OpenAI(api_key=config.api_key, base_url=base_url)
        
        elif self.provider == "groq":
            from groq import Groq
            self.client = Groq(api_key=config.api_key)
        
        elif self.provider == "gemini":
            import google.generativeai as genai
            genai.configure(api_key=config.api_key)
            self.client = genai.GenerativeModel(config.model_name)
        
        else:
            raise ValueError(f"Неподдерживаемый провайдер: {self.provider}")
    
    def extract_header(self, header_text: str) -> Dict[str, Any]:
        log_step("HEADER_EXTRACTION_START", provider=self.provider, text_length=len(header_text))
        
        prompt = f"\nВот фрагмент шапки банковской выписки:\n\n{header_text}\n\nИзвлеки реквизиты должника и верни JSON согласно инструкциям выше.\nВАЖНО: верни ТОЛЬКО JSON, без дополнительных комментариев.\n"
        last_error = None
        last_response = None
        max_retries = 5
        
        for attempt in range(1, max_retries + 1):
            try:
                request_prompt = prompt
                is_repair = False
                if attempt > 1 and last_error and last_response:
                    request_prompt = build_repair_prompt(
                        kind="header",
                        source_text=header_text,
                        previous_response=last_response,
                        error=str(last_error),
                    )
                    is_repair = True
                
                result_text = self.send(system_prompt=HEADER_SYSTEM_PROMPT, user_prompt=request_prompt)
                last_response = result_text
                
                log_llm_response(f"Header LLM response: {result_text[:200]}")
                
                parsed = parse_json_safely(result_text)
                normalized = normalize_header(parsed if isinstance(parsed, dict) else {})
                
                if self.run_logs:
                    self.run_logs.log_header_attempt(
                        attempt,
                        request_prompt,
                        result_text,
                        None,
                        meta={
                            "provider": self.provider,
                            "model": self.config.model_name,
                            "is_repair": is_repair,
                        },
                    )
                log_step("HEADER_EXTRACTION_DONE", attempt=attempt)
                return normalized
            
            except (json.JSONDecodeError, ValueError) as e:
                last_error = e
                if self.run_logs:
                    self.run_logs.log_header_attempt(
                        attempt,
                        request_prompt,
                        last_response or "",
                        str(e),
                        meta={
                            "provider": self.provider,
                            "model": self.config.model_name,
                            "is_repair": attempt > 1,
                        },
                    )
                log_step("LLM_RETRY_ATTEMPT", attempt=attempt, max_retries=max_retries, error=str(e)[:200])
                if attempt < max_retries:
                    wait_time = retry_sleep_seconds(attempt)
                    log_step("LLM_RETRY_WAIT", seconds=wait_time)
                    time.sleep(wait_time)
        
        log_error(last_error, "Header extraction failed after retries")
        return normalize_header({})
    
    def extract_operations(
        self,
        operations_text: str,
        batch_number: int,
        max_retries: int = 7,
    ) -> List[Dict[str, Any]]:
        log_step("OPERATIONS_EXTRACTION_START", provider=self.provider, lines=operations_text.count('\n'), chars=len(operations_text))
        log_llm_request(f"Operations prompt (first 100 lines): {operations_text.split(chr(10))[:100]}")
        
        prompt = f"\nВот фрагмент таблицы операций из банковской выписки:\n\n{operations_text}\n\nИзвлеки все операции из этого фрагмента и верни JSON-массив согласно инструкциям выше.\nВАЖНО: верни ТОЛЬКО JSON-массив, без дополнительных комментариев.\n"
        
        last_error = None
        last_response = None
        if self.run_logs:
            self.run_logs.log_batch_input(batch_number, operations_text)
        
        for attempt in range(1, max_retries + 1):
            try:
                request_prompt = prompt
                is_repair = False
                if attempt > 1 and last_error and last_response:
                    request_prompt = build_repair_prompt(
                        kind="operations",
                        source_text=operations_text,
                        previous_response=last_response,
                        error=str(last_error),
                    )
                    is_repair = True
                
                result_text = self.send(system_prompt=OPERATIONS_SYSTEM_PROMPT, user_prompt=request_prompt)
                last_response = result_text
                
                log_llm_response(f"Operations response preview: {result_text[:200]}")
                
                parsed = parse_json_safely(result_text)
                
                if not isinstance(parsed, list):
                    raise ValueError(f"Expected list, got {type(parsed)}")
                
                log_step("OPERATIONS_EXTRACTION_DONE", operations_count=len(parsed))
                
                if attempt > 1:
                    log_step("LLM_RETRY_SUCCESS", attempt=attempt)
                
                normalized = normalize_operations(parsed)
                if self.run_logs:
                    self.run_logs.log_batch_attempt(
                        batch_number,
                        attempt,
                        request_prompt,
                        result_text,
                        None,
                        meta={
                            "provider": self.provider,
                            "model": self.config.model_name,
                            "is_repair": is_repair,
                        },
                    )
                return normalized
                
            except (json.JSONDecodeError, ValueError) as e:
                last_error = e
                if self.run_logs:
                    self.run_logs.log_batch_attempt(
                        batch_number,
                        attempt,
                        request_prompt,
                        last_response or "",
                        str(e),
                        meta={
                            "provider": self.provider,
                            "model": self.config.model_name,
                            "is_repair": attempt > 1,
                        },
                    )
                log_step("LLM_RETRY_ATTEMPT", attempt=attempt, max_retries=max_retries, error=str(e)[:200])
                
                if attempt < max_retries:
                    wait_time = retry_sleep_seconds(attempt)
                    log_step("LLM_RETRY_WAIT", seconds=wait_time)
                    time.sleep(wait_time)
        
        error_msg = f"Batch extraction failed after {max_retries} attempts (invalid JSON). Will fallback to recovery."
        log_error(last_error, error_msg)
        log_step("LLM_BATCH_FAILED", batch=batch_number, max_retries=max_retries, error=str(last_error)[:200])
        log_step("FAILED_BATCH_TEXT", text_preview=operations_text[:500])
        return []

    def send(self, system_prompt: str, user_prompt: str) -> str:
        if self.provider == "claude":
            response = self.client.messages.create(
                model=self.config.model_name,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return response.content[0].text
        
        if self.provider in ["openai", "deepseek", "groq"]:
            response = self.client.chat.completions.create(
                model=self.config.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
            )
            return response.choices[0].message.content
        
        if self.provider == "gemini":
            full_prompt = f"{system_prompt}\n\n{user_prompt}"
            response = self.client.generate_content(full_prompt)
            return response.text
        
        raise ValueError(f"Неподдерживаемый провайдер: {self.provider}")


def parse_json_safely(text: str) -> Any:
    text = re.sub(r'^```json\s*', '', text.strip())
    text = re.sub(r'\s*```$', '', text.strip())
    text = re.sub(r'^```\s*', '', text.strip())

    json_part = extract_json_substring(text)
    if json_part is None:
        raise json.JSONDecodeError("No JSON found in LLM response", text, 0)

    try:
        return json.loads(json_part)
    except json.JSONDecodeError as e:
        log_error(e, f"JSON parsing failed. Response: {text[:1000]}")
        raise


def extract_json_substring(text: str) -> str | None:
    start_candidates = [text.find("["), text.find("{")]
    start_candidates = [i for i in start_candidates if i != -1]
    if not start_candidates:
        return None
    start = min(start_candidates)

    end_candidates = [text.rfind("]"), text.rfind("}")]
    end_candidates = [i for i in end_candidates if i != -1]
    if not end_candidates:
        return None
    end = max(end_candidates)

    if end <= start:
        return None
    return text[start : end + 1]


def build_repair_prompt(kind: str, source_text: str, previous_response: str, error: str) -> str:
    title = "шапки" if kind == "header" else "операций"
    return (
        "Ты вернул НЕВАЛИДНЫЙ JSON.\n"
        "Твоя задача: вернуть ВАЛИДНЫЙ JSON, НЕ МЕНЯЯ ДАННЫЕ и НЕ ТЕРЯЯ НИЧЕГО.\n"
        "Правила:\n"
        "- нельзя удалять/обрезать/искажать суммы, ИНН, счета, номера документов, назначение платежа\n"
        "- кавычки внутри строк обязательно экранируй как \\\\\" (или используй \\u0022)\n"
        "- переносы строк внутри строк обязательно экранируй как \\\\n\n"
        f"Ошибка парсинга: {error}\n\n"
        f"Фрагмент {title} (исходный текст):\n{source_text}\n\n"
        f"Твой предыдущий ответ (почини его, не потеряв данные):\n{previous_response}\n\n"
        "Верни ТОЛЬКО валидный JSON, без комментариев.\n"
    )
