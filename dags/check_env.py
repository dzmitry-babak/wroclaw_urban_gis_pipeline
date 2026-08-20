from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="check_env",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["geo", "debug"],
)
def check_env():
    @task
    def show_versions():
        import geopandas as gpd
        import osmnx as ox
        import sqlalchemy

        print("geopandas:", gpd.__version__)
        print("osmnx:", ox.__version__)
        print("sqlalchemy:", sqlalchemy.__version__)

    show_versions()


check_env()
