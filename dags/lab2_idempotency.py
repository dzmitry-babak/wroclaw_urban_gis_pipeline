from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, task
from sqlalchemy import create_engine, text


def engine():
    # функция, а не глобальный create_engine: движок создаётся внутри таска,
    # а не при каждом парсинге файла dag-processor'ом
    return create_engine(os.environ["GEO_DSN"])


@dag(
    dag_id="lab2_idempotency",
    schedule="@daily",
    start_date=datetime(2026, 8, 10),
    catchup=False,
    tags=["lab"],
)
def lab2_idempotency():

    @task
    def prepare() -> None:
        with engine().begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS lab_naive (
                    dt date, city text, rows int
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS lab_idem (
                    dt date, city text, rows int,
                    PRIMARY KEY (dt, city)
                )
            """))

    @task
    def extract(**context) -> list[dict]:
        # данные детерминированы от интервала, а не от now()
        day = context["data_interval_start"].date().isoformat()
        return [
            {"dt": day, "city": "Wroclaw", "rows": 1200},
            {"dt": day, "city": "Poznan", "rows": 830},
        ]

    @task
    def load_naive(batch: list[dict]) -> None:
        # так пишет большинство — и получает дубли при каждом retry
        with engine().begin() as conn:
            conn.execute(
                text("INSERT INTO lab_naive (dt, city, rows) VALUES (:dt, :city, :rows)"),
                batch,
            )

    @task
    def load_idempotent(batch: list[dict], **context) -> None:
        # delete-insert по партиции интервала, обе операции в одной транзакции
        day = context["data_interval_start"].date().isoformat()
        with engine().begin() as conn:
            conn.execute(text("DELETE FROM lab_idem WHERE dt = :dt"), {"dt": day})
            conn.execute(
                text("INSERT INTO lab_idem (dt, city, rows) VALUES (:dt, :city, :rows)"),
                batch,
            )

    @task
    def report() -> None:
        with engine().connect() as conn:
            naive = conn.execute(text("SELECT count(*) FROM lab_naive")).scalar()
            idem = conn.execute(text("SELECT count(*) FROM lab_idem")).scalar()
        print(f"lab_naive: {naive} строк | lab_idem: {idem} строк")

    batch = extract()
    prepare() >> batch
    [load_naive(batch), load_idempotent(batch)] >> report()


lab2_idempotency()