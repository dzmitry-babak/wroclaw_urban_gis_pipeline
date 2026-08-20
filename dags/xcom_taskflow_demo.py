from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="xcom_taskflow_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "xcom"],
)
def xcom_taskflow_demo():

    @task
    def extract() -> list[dict]:
        # return = неявный xcom_push под ключом "return_value"
        return [
            {"city": "Wroclaw", "rows": 1200},
            {"city": "Poznan", "rows": 830},
            {"city": "Krakow", "rows": 1540},
        ]

    @task(multiple_outputs=True)
    def summarize(batches: list[dict]) -> dict:
        # multiple_outputs=True → каждый ключ словаря становится отдельным XCom
        return {
            "total_rows": sum(b["rows"] for b in batches),
            "cities": len(batches),
        }

    @task
    def transform(batch: dict) -> str:
        return f"{batch['city']}: {batch['rows']} rows"

    @task
    def report(total_rows: int, cities: int, lines: list[str]) -> None:
        print(f"{cities} городов, {total_rows} строк всего")
        for line in lines:
            print(line)

    batches = extract()
    stats = summarize(batches)
    lines = transform.expand(batch=batches)   # 3 параллельных таска, по одному на элемент

    report(
        total_rows=stats["total_rows"],
        cities=stats["cities"],
        lines=lines,
    )


xcom_taskflow_demo()