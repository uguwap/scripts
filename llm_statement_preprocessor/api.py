import os
import json
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from logger import log_step, log_error
from services import StatementPreprocessor, PreprocessorFactory, ProcessingResult
from excel_builder import build_standard_excel


UPLOADS_DIR = Path(__file__).parent / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)


app = FastAPI(
    title="LLM Statement Preprocessor",
    description="Преобразование нечитаемых банковских выписок в стандартный формат через LLM API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


CHECKPOINT_STORAGE = {}


@app.get("/")
async def root():
    return {
        "service": "LLM Statement Preprocessor",
        "version": "1.0.0",
        "status": "running",
        "swagger_ui": "/docs"
    }


@app.get("/health")
async def health_check():
    api_key = os.getenv("LLM_API_KEY")
    llm_enabled = os.getenv("LLM_ENABLED", "1").strip() not in ("0", "false", "False", "no", "NO")
    
    if llm_enabled and not api_key:
        return {"status": "unhealthy", "error": "LLM_API_KEY not set (LLM_ENABLED=1)"}
    
    return {
        "status": "healthy",
        "llm_provider": os.getenv("LLM_PROVIDER", "deepseek"),
        "llm_enabled": llm_enabled,
        "api_key_present": bool(api_key),
    }


@app.get("/download/{filename}")
async def download_file(filename: str):
    for stored_filename, file_path in CHECKPOINT_STORAGE.items():
        if stored_filename == filename:
            if os.path.exists(file_path):
                return FileResponse(
                    path=file_path,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=filename
                )
    
    raise HTTPException(status_code=404, detail=f"Файл {filename} не найден")


@app.post("/preprocess/statement")
async def preprocess_statement(
    file: UploadFile = File(...),
    enable_checkpoints: bool = Form(default=True),
    checkpoint_interval: int = Form(default=50),
    strict_mode: bool = Form(default=True),
):
    log_step("API_REQUEST", filename=file.filename, checkpoints=enable_checkpoints)
    
    temp_input = None
    checkpoint_dir = None
    run_logs = None
    
    try:
        validate_file_format(file.filename)
        log_step("FILE_VALIDATION_OK", filename=file.filename)
        
        temp_input = await save_upload_to_temp(file)
        log_step("FILE_SAVED_TO_TEMP", path=temp_input)
        
        checkpoint_dir = str(UPLOADS_DIR) if enable_checkpoints else None
        if checkpoint_dir:
            log_step("CHECKPOINT_DIR_SET", path=checkpoint_dir)

        from run_logs import RunLogs
        run_logs = RunLogs(LOGS_DIR)
        log_step("RUN_LOGS_DIR", path=str(run_logs.ctx.run_dir))

        preprocessor = create_preprocessor()
        log_step("PREPROCESSOR_CREATED")
        
        result = preprocessor.process_file(
            temp_input, 
            checkpoint_dir=checkpoint_dir,
            checkpoint_every=checkpoint_interval,
            strict_mode=strict_mode,
            run_logs=run_logs,
        )
        
        if not result.is_success:
            final_excel_path = create_excel_output(preprocessor, result, str(UPLOADS_DIR))
            register_files_for_download(result, final_excel_path)
            return JSONResponse(
                {
                    "status": "error",
                    "message": "Не удалось извлечь ни одной операции. Файлы и логи сохранены.",
                    "logs_dir": str(run_logs.ctx.run_dir),
                    "recovery_batches": build_recovery_batches_summary(run_logs.ctx.run_dir),
                    "final": {
                        "filename": "final_all_operations.xlsx",
                        "download_url": f"/download/{Path(final_excel_path).name}",
                        "operations_count": result.valid_operations,
                    },
                },
                status_code=200,
            )
        
        final_excel_path = create_excel_output(preprocessor, result, str(UPLOADS_DIR))
        
        register_files_for_download(result, final_excel_path)
        
        response = build_checkpoint_response(result, final_excel_path, checkpoint_dir)
        data = json.loads(response.body.decode("utf-8"))
        data["logs_dir"] = str(run_logs.ctx.run_dir)
        data["recovery_batches"] = build_recovery_batches_summary(run_logs.ctx.run_dir)
        return JSONResponse(data, status_code=200)
    
    except HTTPException as e:
        log_error(e, f"HTTP error: {e.detail}")
        raise
    
    except Exception as e:
        log_error(e, "Unexpected error in API")
        final_excel_path = create_failsafe_excel(str(UPLOADS_DIR))
        CHECKPOINT_STORAGE[Path(final_excel_path).name] = final_excel_path
        logs_dir = str(run_logs.ctx.run_dir) if run_logs else None
        recovery_batches = build_recovery_batches_summary(run_logs.ctx.run_dir) if run_logs else []
        return JSONResponse(
            {
                "status": "error",
                "message": "Неожиданная ошибка при обработке. Финальный Excel и логи сохранены.",
                "error": str(e),
                "logs_dir": logs_dir,
                "recovery_batches": recovery_batches,
                "final": {
                    "filename": "final_all_operations.xlsx",
                    "download_url": f"/download/{Path(final_excel_path).name}",
                    "operations_count": 0,
                },
            },
            status_code=200,
        )
    
    finally:
        if temp_input and os.path.exists(temp_input):
            os.unlink(temp_input)


def validate_file_format(filename: str) -> None:
    file_extension = Path(filename).suffix.lower()
    supported_formats = ['.xlsx', '.xls', '.rtf', '.csv']
    
    if file_extension not in supported_formats:
        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемый формат файла: {file_extension}"
        )


async def save_upload_to_temp(file: UploadFile) -> str:
    file_extension = Path(file.filename).suffix.lower()
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as tmp:
        content = await file.read()
        tmp.write(content)
        return tmp.name


def create_preprocessor() -> StatementPreprocessor:
    try:
        return PreprocessorFactory.create_from_env()
    except ValueError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка конфигурации LLM: {str(e)}"
        )


def build_checkpoint_response(result: ProcessingResult, final_path: str, checkpoint_dir: str = None) -> JSONResponse:
    response_data = {
        "status": "success",
        "stats": result.stats,
        "final": {
            "filename": "final_all_operations.xlsx",
            "download_url": f"/download/{Path(final_path).name}",
            "operations_count": result.valid_operations
        }
    }
    
    if checkpoint_dir and result.checkpoints:
        response_data["checkpoints"] = [
            {
                "batch_range": cp.batch_range,
                "filename": Path(cp.file_path).name,
                "download_url": f"/download/{Path(cp.file_path).name}",
                "operations_count": cp.operations_count
            }
            for cp in result.checkpoints
        ]
    
    return JSONResponse(response_data)


def create_excel_output(preprocessor: StatementPreprocessor, result: ProcessingResult, output_dir: str) -> str:
    final_path = Path(output_dir) / "final_all_operations.xlsx"
    preprocessor.save_to_excel(result, str(final_path))
    return str(final_path)


def create_failsafe_excel(output_dir: str) -> str:
    final_path = Path(output_dir) / "final_all_operations.xlsx"
    build_standard_excel(header={}, operations=[], output_path=str(final_path))
    return str(final_path)


def register_files_for_download(result: ProcessingResult, final_path: str) -> None:
    CHECKPOINT_STORAGE[Path(final_path).name] = final_path
    
    for checkpoint in result.checkpoints:
        filename = Path(checkpoint.file_path).name
        CHECKPOINT_STORAGE[filename] = checkpoint.file_path


def build_recovery_batches_summary(logs_dir: Path) -> list[dict]:
    index_path = logs_dir / "index.json"
    if not index_path.exists():
        return []
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    batches = index.get("batches") or {}
    result: list[dict] = []
    for batch_number_str, batch_info in batches.items():
        if not isinstance(batch_info, dict):
            continue
        recovery_rel = batch_info.get("recovery_path")
        if not recovery_rel:
            continue
        recovery_path = logs_dir / Path(recovery_rel)
        payload = {
            "batch_number": int(batch_number_str) if str(batch_number_str).isdigit() else batch_number_str,
            "recovery_path": str(recovery_path),
        }
        if recovery_path.exists():
            try:
                recovery_json = json.loads(recovery_path.read_text(encoding="utf-8"))
                payload.update(
                    {
                        "reason": recovery_json.get("reason"),
                        "operations_count": recovery_json.get("operations_count"),
                        "debit_sum": recovery_json.get("debit_sum"),
                        "credit_sum": recovery_json.get("credit_sum"),
                    }
                )
            except Exception:
                pass
        result.append(payload)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000, log_level="info")
