from main_file import run_sql
from pathlib import Path
import folium
import pandas as pd
import numpy as np
from folium import plugins


def main():
    CONFIG = Path(__file__).resolve().parent / "config.env"

    rows = run_sql("""
            SELECT 
                ST_Y(s.geom) AS lat,
                ST_X(s.geom) AS lon,
                s.name,
                CAST(AVG(n.laeq_day) AS NUMERIC(3,1)) AS laeq_day,
                CAST(AVG(n.laeq_night) AS NUMERIC(3,1)) AS laeq_night
            FROM stations s
            LEFT JOIN noise_level_d n ON n.station_id = s.station_id
            GROUP BY s.station_id, s.geom, s.name
            ORDER BY s.station_id;

    """)

    df = pd.DataFrame(
        rows, columns=["lat", "lon", "name", "laeq_day", "laeq_night"])

    m = folium.Map(location=[df["lat"].mean(), df["lon"].mean()],
                   zoom_start=13,
                   zoom_control=False,
                   scrollWheelZoom=False,
                   dragging=False,
                   tiles=None
                   )

    seoul_bounds = [[37.38, 126.85], [37.70, 127.15]]
    folium.Rectangle(
        bounds=seoul_bounds,
        color=None,
        fill=True,
        fill_color="lightgreen",
        fill_opacity=0.18,
        z_index=1
    ).add_to(m)

    folium.raster_layers.TileLayer(
        attr="Seoul Noise Map",
        name="Base Map",
        opacity=0.9,
        control=False,
    ).add_to(m)

    folium.Rectangle(
        bounds=[[37.38, 126.85], [37.70, 127.15]],
        color=None,
        fill=True,
        fill_color="lightgreen",
        fill_opacity=0.25,
        z_index=0
    ).add_to(m)

    heat_data = [[r["lat"], r["lon"], 1] for _, r in df.iterrows()]

    plugins.HeatMap(
        heat_data,
        min_opacity=0.5,
        max_zoom=12,
        radius=65,
        blur=50,
        gradient={0.85: 'green', 0.9: 'yellow', 0.95: 'orange', 1: 'red'},
        z_index=2
    ).add_to(m)

    for _, row in df.iterrows():
        popup_text = (
            f"<b>{row['name']}</b><br>"
            f"🌞 Day: {row['laeq_day']} dB<br>"
            f"🌙 Night: {row['laeq_night']} dB<br>"
            f"📊 Average: {round((row['laeq_day'] + row['laeq_night']) / 2, 1)} dB"
        )

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=8,
            color="red",
            fill=True,
            fill_opacity=0.9,
            popup=folium.Popup(popup_text, max_width=300),
            tooltip=row["name"]
        ).add_to(m)
    m.fit_bounds(seoul_bounds)
    m.options['minZoom'] = 13
    m.options['maxZoom'] = 13

    out = "web/step_noise_heatmap.html"
    m.save(out)
    print(f"✅ Карта сохранена: {out}")


if __name__ == "__main__":
    main()
