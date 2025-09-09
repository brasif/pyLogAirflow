import json
import logging
import time
from typing import Any, Callable, Dict, Optional, Tuple

# Airflow imports are optional at import-time to allow usage outside workers.
try:
    from airflow.utils.context import get_current_context  # Airflow 2.x
except Exception:  # ImportError or runtime error when not inside a task
    get_current_context = None  # type: ignore


def _safe_context() -> Dict[str, Any]:
    """Return Airflow context dict if available, else an empty dict."""
    if get_current_context is None:
        return {}
    try:
        return dict(get_current_context() or {})
    except Exception:
        # We may be importing or running outside of a task
        return {}


def _extract_runtime_fields(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Pick useful runtime fields from Airflow context."""
    fields: Dict[str, Any] = {}
    ti = ctx.get("ti") or ctx.get("task_instance")
    task = ctx.get("task")
    dag = ctx.get("dag")
    dag_run = ctx.get("dag_run")

    # Core IDs
    fields["dag_id"] = getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None)
    fields["task_id"] = getattr(task, "task_id", None) or getattr(ti, "task_id", None)

    # Attempt/run metadata
    fields["try_number"] = getattr(ti, "try_number", None)
    fields["map_index"] = getattr(ti, "map_index", None)
    fields["run_id"] = getattr(dag_run, "run_id", None)
    fields["execution_date"] = getattr(getattr(dag_run, "logical_date", None), "isoformat", lambda: None)()
    fields["owner"] = getattr(getattr(task, "owner", None), "name", None) if hasattr(getattr(task,"owner",None), "name") else getattr(task, "owner", None)

    return {k: v for k, v in fields.items() if v is not None}


class _JsonFormatter(logging.Formatter):
    """Structured JSON log lines for easier parsing in Airflow/Elastic/etc."""
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": int(record.created * 1000),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        # Include any pre-attached dict in record.__dict__["extra_json"]
        extra_json = getattr(record, "extra_json", None)
        if isinstance(extra_json, dict):
            base.update(extra_json)
        # Attach exception info if present
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)


class Log:
    """
    Airflow-aware logger.

    - When running inside a task, it writes to the task's standard log output
      (picked up by Airflow log handlers).
    - Outside Airflow, it logs to a standard StreamHandler.

    It emits JSON lines by default for easy ingestion.
    """
    def __init__(
        self,
        name: str = "pyLogAirflow",
        json_format: bool = True,
        extra_fields: Optional[Dict[str, Any]] = None,
        push_xcom: bool = False
    ) -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        self.push_xcom = push_xcom

        # Only add our own handler if logger has none (avoids duplicate lines in Airflow)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            if json_format:
                handler.setFormatter(_JsonFormatter())
            else:
                handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
            self._logger.addHandler(handler)

        self._base_extra = extra_fields or {}

    # ---- Public logging methods -------------------------------------------------
    def info(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.INFO, msg, status="OK", **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.WARNING, msg, status="WARN", **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, msg, status="ERROR", **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, msg, status="ERROR", exc_info=True, **kwargs)

    # Compatibility helpers similar to "success/fail" semantics
    def success(self, msg: str = "Task step succeeded", **kwargs: Any) -> None:
        self._emit(logging.INFO, msg, status="SUCCESS", **kwargs)

    def fail(self, msg: str = "Task step failed", **kwargs: Any) -> None:
        self._emit(logging.ERROR, msg, status="FAIL", **kwargs)

    # ---- Internals --------------------------------------------------------------
    def _emit(self, level: int, msg: str, *, status: str, exc_info: bool=False, **kwargs: Any) -> None:
        ctx = _safe_context()
        ctx_fields = _extract_runtime_fields(ctx)
        extra_json = dict(self._base_extra)
        extra_json.update(ctx_fields)
        extra_json["status"] = status
        extra_json.update(kwargs)

        # Include dict on the record
        self._logger.log(level, msg, extra={"extra_json": extra_json}, exc_info=exc_info)

        # Optionally push XCom with the last status/message (useful for downstream tasks)
        if self.push_xcom and ctx:
            try:
                ti = ctx.get("ti") or ctx.get("task_instance")
                if ti is not None:
                    ti.xcom_push(key="pyLogAirflow.last", value={"status": status, "message": msg, **ctx_fields, **kwargs})
            except Exception:
                # Avoid hard failures caused by XCom issues
                pass


# ---------- Decorator to auto-log function tasks --------------------------------
def log_task(fn: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator for Python task callables. Logs start/end and duration.
    Usage:

        @task
        @log_task
        def my_func(...): ...
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        log = Log()
        start = time.perf_counter()
        log.info(f"Starting: {fn.__name__}")
        try:
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            log.success(f"Finished: {fn.__name__}", duration_sec=round(elapsed, 3))
            return result
        except Exception as e:
            elapsed = time.perf_counter() - start
            log.exception(f"Error in: {fn.__name__}", duration_sec=round(elapsed, 3))
            raise
    # Preserve function metadata if desired (optional, keep simple)
    wrapper.__name__ = fn.__name__
    wrapper.__doc__ = fn.__doc__
    wrapper.__qualname__ = fn.__qualname__
    return wrapper


# ---------- Callback helpers for DAG default_args --------------------------------
def make_callbacks(
    name: str = "pyLogAirflow",
    push_xcom: bool = False
) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[Dict[str, Any]], None]]:
    """
    Factory that returns (on_success_callback, on_failure_callback).

    Example:
        success_cb, failure_cb = make_callbacks()
        with DAG(..., default_args={"on_success_callback": success_cb,
                                    "on_failure_callback": failure_cb}):
            ...
    """
    def _on_success(context: Dict[str, Any]) -> None:
        log = Log(name=name, push_xcom=push_xcom)
        fields = _extract_runtime_fields(context)
        log.success("Task succeeded", **fields)

    def _on_failure(context: Dict[str, Any]) -> None:
        log = Log(name=name, push_xcom=push_xcom)
        fields = _extract_runtime_fields(context)
        # Airflow passes exception and traceback in context for failure callbacks
        exc = context.get("exception")
        msg = f"Task failed: {type(exc).__name__ if exc else 'Exception'}"
        log.error(msg, **fields)

    return _on_success, _on_failure
