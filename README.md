# pyLogAirflow

Airflow-aware logger with JSON logs, decorator, and success/failure callbacks.

## Install (editable)
```bash
pip install -e ./pyLogAirflow
```

## Quick start
```python
from pyLogAirflow import Log, log_task, make_callbacks
from airflow.decorators import dag, task
from datetime import datetime

success_cb, failure_cb = make_callbacks(push_xcom=True)

@dag(start_date=datetime(2024,1,1), schedule=None,
     default_args={"on_success_callback": success_cb,
                   "on_failure_callback": failure_cb})
def demo_logger():
    @task
    @log_task
    def step():
        log = Log(push_xcom=True)
        log.info("Doing work", extra="value")
        # ...
        log.success("All good")
    step()

demo_logger()
```

## What it does
- Writes task logs to the standard Airflow task log handlers (no custom DB writes).
- Emits structured JSON lines for easy parsing in Elastic/Grafana/Loki/Splunk.
- Adds context fields: `dag_id`, `task_id`, `try_number`, `map_index`, `run_id`, `execution_date`.
- Optional XCom push with last status/message: `key='pyLogAirflow.last'`.
