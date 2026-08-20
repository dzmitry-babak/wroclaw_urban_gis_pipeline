from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def extract(**context) -> list[dict]:
    rows = [
        {"city": "Wroclaw", "rows": 1200},
        {"city": "Poznan", "rows": 830},
        {"city": "Krakow", "rows": 1540},
    ]
    # явный push под своим ключом — рядом с данными кладём метаданные
    context["ti"].xcom_push(key="row_count", value=sum(r["rows"] for r in rows))
    return rows  # плюс неявный push под ключом "return_value"


def load(**context) -> None:
    ti = context["ti"]

    rows = ti.xcom_pull(task_ids="extract")                      # key="return_value" по умолчанию
    row_count = ti.xcom_pull(task_ids="extract", key="row_count")
    started_at = ti.xcom_pull(task_ids="stamp")                  # stdout BashOperator

    print(f"загрузка {len(rows)} городов / {row_count} строк, старт: {started_at}")


with DAG(
    dag_id="xcom_classic_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "xcom"],
):
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    stamp = BashOperator(
        task_id="stamp",
        bash_command="date -u +%Y-%m-%dT%H:%M:%SZ",  # последняя строка stdout уходит в XCom
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    [extract_task, stamp] >> load_task