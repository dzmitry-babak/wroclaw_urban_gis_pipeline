from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="lab1_time",
    schedule="0 3 * * *",              # каждый день в 03:00
    start_date=datetime(2026, 8, 10),
    catchup=True,                       # догнать пропущенное
    max_active_runs=1,                  # догонять по одному, а не все разом
    tags=["lab"],
)
def lab1_time():

    @task
    def show(**context):
        print("logical_date        :", context["logical_date"])
        print("data_interval_start :", context["data_interval_start"])
        print("data_interval_end   :", context["data_interval_end"])
        print("ds (шаблон)         :", context["ds"])
        print("run_id              :", context["run_id"])
        print("try_number          :", context["ti"].try_number)

    show()


lab1_time()