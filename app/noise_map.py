from main_file import run_sql
from pathlib import Path
import folium
import pandas as pd


def main():
    CONFIG = Path(__file__).resolve().parent / "config.env"
    rows = run_sql("""
        SELECT
            ST_Y(geom) AS lat,   
            ST_X(geom) AS lon,   
            name
        FROM stations
        ORDER BY station_id
    """)

    try:
        lats = [r[0] for r in rows]
        lons = [r[1] for r in rows]
        iter_rows = rows
    except Exception:
        lats = rows["lat"].tolist()
        lons = rows["lon"].tolist()
        iter_rows = rows[["lat", "lon", "name"]
                         ].itertuples(index=False, name=None)

    m = folium.Map(location=[37.5663, 126.9779],
                   zoom_start=12, tiles="OpenStreetMap")

    for lat, lon, name in iter_rows:
        folium.Marker([lat, lon], tooltip=str(name)).add_to(m)

    out = "web/step1_simple_map.html"
    m.save(out)
    print("Saved:", out)


if __name__ == "__main__":
    main()
