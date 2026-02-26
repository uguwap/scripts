"""
Конфигурация LLM-парсера выписок
"""
import os
from dataclasses import dataclass
from typing import Literal

LLMProvider = Literal["claude", "openai", "gemini", "deepseek", "groq"]


@dataclass
class Config:
    """Конфигурация для LLM API"""
    llm_provider: LLMProvider
    api_key: str
    model_name: str
    max_tokens: int = 4096
    temperature: float = 0.0  # Детерминированный вывод для финансовых данных
    batch_size: int = 10  # Кол-во строк табличной части на один батч (меньше = выше шанс валидного JSON)
    llm_enabled: bool = True
    
    @classmethod
    def from_env(cls):
        """Загрузка конфигурации из переменных окружения"""
        provider = os.getenv("LLM_PROVIDER", "claude")
        llm_enabled = os.getenv("LLM_ENABLED", "1").strip() not in ("0", "false", "False", "no", "NO")
        
        # Определение модели по умолчанию в зависимости от провайдера
        default_models = {
            "claude": "claude-3-5-sonnet-20241022",
            "openai": "gpt-4-turbo-2024-04-09",
            "gemini": "gemini-1.5-pro",
            "deepseek": "deepseek-chat",
            "groq": "llama-3.1-70b-versatile"
        }
        
        return cls(
            llm_provider=provider,
            api_key=os.getenv("LLM_API_KEY"),
            model_name=os.getenv("LLM_MODEL", default_models.get(provider, "claude-3-5-sonnet-20241022")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "4096")),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")),
            batch_size=int(os.getenv("BATCH_SIZE", "10")),
            llm_enabled=llm_enabled,
        )
    
    def validate(self):
        """Проверка корректности конфигурации"""
        if self.llm_enabled and not self.api_key:
            raise ValueError("API ключ не найден! Установите переменную LLM_API_KEY")
        
        if self.llm_provider not in ["claude", "openai", "gemini", "deepseek", "groq"]:
            raise ValueError(f"Неподдерживаемый провайдер: {self.llm_provider}")
        
        return True

