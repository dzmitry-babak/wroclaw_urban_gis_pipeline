# viz/make_map.py
import math

import geopandas as gpd
from keplergl import KeplerGl
from sqlalchemy import create_engine

DSN = "postgresql://postgres:geo@localhost:5433/geo"
OUT = "docs/kepler_cafes.html"
WIDTH_PX, HEIGHT_PX = 1000, 600


def zoom_for_bounds(minx, miny, maxx, maxy, width_px=WIDTH_PX, height_px=HEIGHT_PX):
    """Подбор zoom под bbox — формула веб-меркатора (как в Google Maps)."""
    world_px = 256

    def lat_rad(lat):
        s = math.sin(math.radians(lat))
        return math.log((1 + s) / (1 - s)) / 2

    lat_fraction = (lat_rad(maxy) - lat_rad(miny)) / math.pi
    lon_fraction = (maxx - minx) / 360

    zoom_lat = math.log2(height_px / world_px / lat_fraction) if lat_fraction > 0 else 20
    zoom_lon = math.log2(width_px / world_px / lon_fraction) if lon_fraction > 0 else 20
    return max(0.0, min(20.0, min(zoom_lat, zoom_lon) - 0.3))   # -0.3 = поля по краям


engine = create_engine(DSN)
gdf = gpd.read_postgis("SELECT name, geometry FROM cafes", engine, geom_col="geometry")
gdf = gdf.to_crs(4326)                      # веб-карты работают в градусах
print(f"из PostGIS прочитано {len(gdf)} кафе")

minx, miny, maxx, maxy = gdf.total_bounds
center_lon, center_lat = (minx + maxx) / 2, (miny + maxy) / 2
zoom = zoom_for_bounds(minx, miny, maxx, maxy)
print(f"bbox: {minx:.4f} {miny:.4f} {maxx:.4f} {maxy:.4f} → center ({center_lat:.4f}, {center_lon:.4f}), zoom {zoom:.2f}")

config = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": center_lat,
            "longitude": center_lon,
            "zoom": zoom,
            "bearing": 0,
            "pitch": 0,
        }
    },
}

m = KeplerGl(height=HEIGHT_PX, config=config)
m.add_data(data=gdf, name="cafes")
m.config = config                            # повторно: add_data сбрасывает mapState
m.save_to_html(file_name=OUT, config=config)
print(f"готово: {OUT}")