from datetime import datetime, timedelta

from airflow.sdk import dag, task

DATA_DIR = "/opt/airflow/data"
PLACE = "Wrocław, Poland"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="wroclaw_mobility",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["geo"],
)
def wroclaw_mobility():
    @task
    def fetch_osm() -> str:
        import osmnx as ox

        gdf = ox.features_from_place(PLACE, tags={"amenity": "cafe"})
        out = f"{DATA_DIR}/cafes_raw.geojson"
        gdf[["name", "geometry"]].to_file(out, driver="GeoJSON")
        print(f"OSM вернул {len(gdf)} объектов → {out}")
        return out

    @task
    def transform_geo(src: str) -> str:
        import geopandas as gpd

        gdf = gpd.read_file(src)
        before = len(gdf)

        gdf = gdf.to_crs(2180)  # СНАЧАЛА в метры (PUWG 1992)

        # полигоны здания-кафе → точка
        poly = gdf.geometry.type.isin(["Polygon", "MultiPolygon"])
        gdf.loc[poly, "geometry"] = gdf.loc[poly, "geometry"].centroid
        gdf = gdf[gdf.geometry.type == "Point"]

        gdf = gdf[gdf["name"].notna()]
        gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty]
        gdf = gdf.drop_duplicates(subset=["name", "geometry"])

        out = f"{DATA_DIR}/cafes_clean.geojson"
        gdf.to_file(out, driver="GeoJSON")
        print(f"transform: {before} → {len(gdf)} объектов, CRS={gdf.crs}")
        return out

    @task
    def load_postgis(src: str) -> int:
        import os

        import geopandas as gpd
        from sqlalchemy import create_engine, text

        engine = create_engine(os.environ["GEO_DSN"])
        gdf = gpd.read_file(src)

        gdf.to_postgis("cafes", engine, if_exists="replace", index=False)

        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_cafes_geom ON cafes USING GIST (geometry)"
            ))
            conn.execute(text("ANALYZE cafes"))
            cnt = conn.execute(text("SELECT COUNT(*) FROM cafes")).scalar()

        print(f"load: в PostGIS {cnt} строк")
        return cnt

    load_postgis(transform_geo(fetch_osm()))


wroclaw_mobility()