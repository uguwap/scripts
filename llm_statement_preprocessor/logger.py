import logging
import sys
import os
from typing import Any

_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)

logging.basicConfig(
    level=_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("statement_preprocessor")

# При root=DEBUG сторонние либы (httpx/httpcore/openai) заливают терминал и тормозят.
# Оставляем наши логи, но режем шум.
for noisy in ("httpx", "httpcore", "openai", "python_multipart"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


def log_step(step_name: str, **kwargs: Any) -> None:
    details = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info(f"[{step_name}] {details}")


def log_llm_request(prompt_preview: str, max_length: int = 200) -> None:
    truncated = prompt_preview[:max_length] + "..." if len(prompt_preview) > max_length else prompt_preview
    logger.debug(f"[LLM REQUEST] {truncated}")


def log_llm_response(response_preview: str, max_length: int = 200) -> None:
    truncated = response_preview[:max_length] + "..." if len(response_preview) > max_length else response_preview
    logger.debug(f"[LLM RESPONSE] {truncated}")


def log_error(error: Exception, context: str = "") -> None:
    logger.error(f"[ERROR] {context}: {type(error).__name__} - {str(error)}", exc_info=True)

