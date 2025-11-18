<p align="right">🌐 Languages: <b>English</b>  <a href="README_ko.md">한국어</a></p>

# NoiseMap — Educational Noise Visualization Project  
---

## 🇬🇧 English

### Project Overview
NoiseMap is an **educational project** focused on learning data processing, peak detection, and geospatial visualization.  
The noise map is generated from publicly available datasets (stored in `data/raw`) and calculated using formulas documented in the `docs/` folder.

The resulting visualization (HTML map) is intended **only for illustration** — it does not account for terrain effects, reflections, weather conditions, or real-world acoustic modeling.

### Features
- Processing raw noise-level data
- PostGIS-ready schema (`stations`, `noise_reading`,`noise_level_d`, `noise_level_h`) with upserts   
- Calculating daily and global peaks  
- Aggregating noise levels by hour  
- Generating an HTML-based noise map  
- Exporting processed results into Excel tables  

### Calculation Method
All formulas, assumptions, and data processing workflow are documented in:  
➡ **`docs/calculation_methods.md`**  

## Repository contents
- **/app/** – data ingest & parsing (pandas → PostgreSQL, PostGIS)
- **/data/raw/** – monthly XLSX sources (by station/sheet)
- **/docs/** - all formulas, assumptions, and data processing workflow
- **/sql/** – schema, indexes, convenience views
- **/web/** – prototype UI (Streamlit / Mapbox)


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
- located **/sql/import.sql/**

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate  
pip install -U pip pandas openpyxl SQLAlchemy psycopg2-binary
python app/main.py
```

## Configuration
Create/edit `app/config.env` and **replace the password with your own**:
