<p align="right">🌐 Languages: <b>English</b> · <a href="README_ko.md">한국어</a></p>

# NoiseMap Seoul — Python / pgSQL Analytics

Build an interactive map of city noise levels that accounts for Seoul’s most popular locations by district, and identify patterns and drivers such as time of day, traffic, weather, and events.

## Repository contents
- **/app/** – data ingest & parsing (pandas → PostgreSQL, PostGIS)
- **/data/raw/** – monthly XLSX sources (by station/sheet)
- **/sql/** – schema, indexes, convenience views
- **/notebooks/** – EDA, modeling, sanity checks
- **/web/** – prototype UI (Streamlit / Mapbox)

## Features (MVP)
- XLSX → long format (date, hour, station, dB) with timezone-aware UTC timestamps  
- PostGIS-ready schema (`stations`, `noise_reading`) with upserts  
- Basic analytics: daily/weekly profiles, night vs day, top districts  
- Hooks for enrichment: hourly weather, traffic, events

## Data pipeline
1. **Parse** monthly Excel files (one sheet per station)  
2. **Normalize** to hourly rows (`date + (hour-1)h`, Asia/Seoul → UTC)  
3. **Load** into PostgreSQL with conflict-safe inserts  
4. (Optional) **Enrich** with weather/traffic/events

## Data sources
- **Noise (primary):** Monthly XLSX exports (Jan–Jun 2025) from **Seoul Open Data Plaza**  
  Dataset: *Road-traffic noise measurements (LEQ), monthly by hour*, with station sheets
  **(시청, 성수, 신촌, 신사)**.  
  File headers include Korean labels such as “측정월”, “측정일\시간”, and hour columns **1–24**.
- The files were downloaded and placed under `data/raw/`. The ETL parses each sheet
  into long-form rows: `(station, date, hour, dB)`.
> Note: dataset names and station list follow the original XLSX; redistribution may be
> subject to the publisher’s terms. Replace or augment with your own sources as needed.

## Database schema (public)
- `stations (station_id PK, name UNIQUE, geom geometry(Point,4326) NULL)`  
- `noise_reading (reading_id PK, station_id FK, ts_utc timestamptz, db_level numeric(5,2), part_of_day text, src_month date GENERATED, UNIQUE(station_id, ts_utc))`

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate    # (Windows: .venv\Scripts\Activate.ps1)
pip install -U pip pandas openpyxl SQLAlchemy psycopg2-binary
python app/main.py
```
# Roadmap
Weather/traffic/event enrichment
Interactive map (Streamlit/Mapbox)
Forecasts & anomaly detection (Prophet/ARIMA + regressors)
CI checks & data quality dashboard

## Configuration
Create/edit `app/config.env` and **replace the password with your own**:
