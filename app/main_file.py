from __future__ import annotations
from contextlib import contextmanager
from typing import Iterator, ContextManager
from pathlib import Path
import re
import pandas as pd
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from functools import lru_cache


# ----------------Connect with config ----------------

def load_db_config(path="app/config.env"):

 # ------support host, port, user, password, database---------

    data_dic = {"host": None, "port": 5432,
                "user": None, "password": None, "database": None}

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            if "=" not in raw or raw.lstrip().startswith("#"):
                continue
            key, value = raw.split("=", 1)
            key = key.strip().lower()
            value = value.strip().strip("'\"")
            if key in data_dic:
                data_dic[key] = int(value) if key == "port" else value

    for k in ("host", "user", "password", "database"):
        if not data_dic[k]:
            raise ValueError(f"В config.env - field not specified: {k}")
    return data_dic

# ----------------Enter personal password ----------------


@lru_cache(maxsize=1)
def connect_engine(config_path: str = "app/config.env", *, echo: bool = False) -> Engine:

    cfg = load_db_config(config_path)
    db_url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        echo=echo,
        future=True,
    )
    return engine


def db_conn(config_path: str = "app/config.env", *, autocommit: bool = True) -> ContextManager[Connection]:
    @contextmanager
    def _ctx() -> Iterator[Connection]:
        engine = connect_engine(config_path)
        if autocommit:
            with engine.begin() as conn:
                yield conn
        else:
            conn = engine.connect()
            try:
                yield conn
            finally:
                conn.close()
    return _ctx()

# --------------Connect to sql----------------


def run_sql(sql: str, params: Optional[dict] = None, *, config_path: str = "app/config.env"):
    """
    Быстрый helper: выполнить запрос и вернуть результат .all() (если это SELECT).
    """
    with db_conn(config_path) as conn:
        res = conn.execute(text(sql), params or {})
        try:
            return res.all()
        except Exception:
            return None

# --------Find year in tables*.csv-----------


def find_year_month(df: pd.DataFrame):

    pat = re.compile(r"(\d{4})\s*년\s*(\d{1,2})\s*월")
    for col in df.columns:
        for v in df[col].astype(str):
            m = pat.search(v)
            if m:
                y, mth = int(m.group(1)), int(m.group(2))
                if 1 <= mth <= 12:
                    return y, mth
    return None

# --------Find hours/stations in tables*.csv-----------


def parse_sheet(df_raw, station_name):
    """
    Return:
      long_df: station_name | date | hour(1..24) | db_level
      hours:   (ex. [1..24])

    """

    df = df_raw.copy().reset_index(drop=True)

    # 1) a row of titles by availability '측정일'
    header_row = None
    for i in range(min(30, len(df))):
        if "측정일" in " ".join(df.iloc[i].astype(str).tolist()):
            header_row = i
            break
    if header_row is None:
        return (pd.DataFrame(columns=["station_name", "date", "hour", "db_level"]), [])

    # 2) titles

    df.columns = df.iloc[header_row]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    # 3) date columns

    date_col = next((c for c in df.columns
                     if isinstance(c, str) and ("측정" in c or "시간" in c)), df.columns[0])

    # 4) dates

    dts = pd.to_datetime(df[date_col], errors="coerce")
    df = df[dts.notna()].copy()
    df["date"] = dts.dt.date

    # 5) hour columns from the header
    hour_cols = []
    hours = []
    for c in df.columns:
        if c in (date_col, "date"):
            continue
        try:
            # "1", 1, "1.0", 1.0
            h = int(round(float(str(c).strip())))
            if 1 <= h <= 24:
                hour_cols.append(c)
                hours.append(h)
        except Exception:
            pass

    hours = sorted(set(hours))
    if not hour_cols:
        return (pd.DataFrame(columns=["station_name", "date", "hour", "db_level"]), [])

    # 6) wide -> long
    long_df = df.melt(id_vars=["date"], value_vars=hour_cols,
                      var_name="hour_raw", value_name="db_level")

    # 7) normalization
    long_df["hour"] = pd.to_numeric(
        long_df["hour_raw"], errors="coerce").round().astype("Int64")
    long_df.drop(columns=["hour_raw"], inplace=True)

    long_df["db_level"] = pd.to_numeric(
        long_df["db_level"].astype(str).str.replace(",", "."), errors="coerce"
    ).round(2)

    long_df = long_df.dropna(subset=["hour", "db_level"])
    long_df["station_name"] = station_name
    long_df["hour"] = long_df["hour"].astype(int)

    return long_df[["station_name", "date", "hour", "db_level"]], hours


# ------Create database/tables-------

def ensure_tables(conn):
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stations(
            station_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            geom geometry(Point,4326)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS noise_reading(
          reading_id  BIGSERIAL PRIMARY KEY,
          station_id  INT NOT NULL,
          ts_utc      TIMESTAMPTZ NOT NULL,
          db_level    NUMERIC(5,2) NOT NULL,
          part_of_day TEXT,
          src_month   DATE GENERATED ALWAYS AS
            (date_trunc('month', ts_utc AT TIME ZONE 'UTC')::date) STORED,
        CONSTRAINT fk_noise_station FOREIGN KEY (station_id)
            REFERENCES stations(station_id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT uq_noise_station_ts UNIQUE (station_id, ts_utc)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS noise_level_d (
            station_id    INT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
            d_kst         DATE NOT NULL,
            laeq_day      NUMERIC(6,2),
            laeq_night    NUMERIC(6,2),
            created_at    TIMESTAMP DEFAULT now(),
            updated_at    TIMESTAMP,
        PRIMARY KEY (station_id, d_kst)
        );

    """))
    conn.execute(text("""CREATE INDEX IF NOT EXISTS
                      idx_noise_reading_station_ts ON noise_reading (station_id, ts_utc););
    """))
    conn.execute(text("""CREATE INDEX idx_noise_ts ON noise_reading(ts_utc););
    """))
    conn.execute(text("""CREATE INDEX idx_noise_station ON noise_reading(station_id););
    """))
    conn.execute(text("""
        ALTER TABLE
            stations
        ADD
            CONSTRAINT uq_stations_name UNIQUE (name);
        );
    """))
    conn.execute(text("""
        ALTER TABLE
            noise_reading
        ADD
            CONSTRAINT uq_noise_station_ts UNIQUE (station_id, ts_utc);          
        );
    """))


def upsert_stations(names, conn):
    for n in sorted(set(names)):
        conn.execute(text("""
            INSERT INTO stations(name, geom)
            VALUES (:n, ST_SetSRID(ST_MakePoint(126.9780, 37.5665), 4326))
            ON CONFLICT (name) DO NOTHING;
        """), {"n": n})


def insert_measurements(df_all, conn):
    """
    Expect for columns: station_name (TEXT), date (DATE), hour (1..24), db_level (NUMERIC).
    Collects local time Asia/Seoul: date + (hour-1)h → translate into UTC.

    """
    conn.execute(text("DROP TABLE IF EXISTS _noise_tmp;"))
    conn.execute(text("""
        CREATE TEMP TABLE _noise_tmp(
          station_name TEXT,
          d DATE,
          hour INT,
          db_level NUMERIC(5,2)
        ) ON COMMIT DROP;
    """))

    df_tmp = df_all.rename(columns={"date": "d"})
    df_tmp.to_sql("_noise_tmp", con=conn, if_exists="append", index=False)

    conn.execute(text("""
        INSERT INTO noise_reading(station_id, ts_utc, db_level, part_of_day)
        SELECT s.station_id,
               ((t.d + (t.hour-1) * INTERVAL '1 hour')::timestamp
                AT TIME ZONE 'Asia/Seoul') AS ts_utc,
               t.db_level,
               CASE WHEN t.hour BETWEEN 7 AND 21 THEN 'day' ELSE 'night' END
        FROM _noise_tmp t
        JOIN stations s ON s.name = t.station_name
        ON CONFLICT (station_id, ts_utc) DO UPDATE
          SET db_level = EXCLUDED.db_level,
              part_of_day = EXCLUDED.part_of_day;
    """))
    # ---Дневные/ночные ЭУШ----
    conn.execute(text("""
        INSERT INTO noise_level_d (station_id, d_kst, laeq_day, laeq_night, created_at, updated_at)
        SELECT station_id, d_kst, laeq_day, laeq_night, now(), now()
        FROM src_daynight
        ON CONFLICT (station_id, d_kst) DO UPDATE
        SET laeq_day   = EXCLUDED.laeq_day,
            laeq_night = EXCLUDED.laeq_night,
            updated_at = now();
    """))
    # ----Эквивалентные почасовые ур.шума----
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS noise_level_h (
            station_id   INT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
            ts_hour_kst  TIMESTAMP NOT NULL,     -- начало часа (KST)
            n_samples    INT NOT NULL,
            laeq         NUMERIC(6,2) NOT NULL,  -- LAeq за час
            created_at   TIMESTAMP DEFAULT now(),
            updated_at   TIMESTAMP,
        PRIMARY KEY (station_id, ts_hour_kst)
        );
    """))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_h_station_ts  ON noise_level_h(station_id, ts_hour_kst);"))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_r_station_ts  ON noise_reading (station_id, ts_utc);"))
    # ---Время с наивысшими показателями---
    conn.execute(text("""
                WITH h AS (
        SELECT
            r.station_id,
            date_trunc('hour', r.ts_utc AT TIME ZONE 'Asia/Seoul') AS ts_hour_kst,
            COUNT(*)                                               AS n_samples,
            10*LOG10( AVG(POWER(10, r.db_level/10.0)) )            AS laeq
        FROM noise_reading r
        WHERE r.db_level IS NOT NULL
            AND (:from_utc IS NULL OR r.ts_utc >= :from_utc)
            AND (:to_utc   IS NULL OR r.ts_utc   <  :to_utc)
        GROUP BY r.station_id, date_trunc('hour', r.ts_utc AT TIME ZONE 'Asia/Seoul')
        )
        INSERT INTO noise_level_h (station_id, ts_hour_kst, n_samples, laeq, created_at, updated_at)
        SELECT station_id, ts_hour_kst, n_samples, laeq, now(), now()
        FROM h
        ON CONFLICT (station_id, ts_hour_kst) DO UPDATE
        SET n_samples = EXCLUDED.n_samples,
            laeq      = EXCLUDED.laeq,
            updated_at= now();
    """))


# def create

# ------------- main -------------


def main():
    try:

        engine = connect_engine()
        with engine.begin() as conn:
            ensure_tables(conn)

        RAW_DIR = Path("data/raw")
        files = sorted(RAW_DIR.glob("*.xlsx"))
        print("Files:", [p.name for p in files])
        for path in files:
            print("→", path)

            # 1) read the book and all the sheets as "raw" tables
            xls = pd.ExcelFile(path)
            sheets = {name: xls.parse(name, header=None)
                      for name in xls.sheet_names}

            # 2) parse each sheet
            frames = []
            for sheet_name, df in sheets.items():
                long_df, hours = parse_sheet(df, sheet_name)
                print(f"  - {sheet_name}: parsed {len(long_df)} rows")
                frames.append(long_df)

            # 3) if it is empty, skip the file
            total = sum(len(x) for x in frames)
            if total == 0:
                print(f"SKIP (no data): {path}")
                continue

            # 4) concat
            all_data = pd.concat(frames, ignore_index=True)

            # 5) (optional) we clean the names of the stations: remove the suffix "(시간별)"
            all_data["station_name"] = (
                all_data["station_name"]
                .astype(str)
                .str.replace(r"\(시간별\)", "", regex=True)
                .str.strip()
            )

            # 6) inserting into the database (upserts)
            with Engine.begin() as conn:
                upsert_stations(all_data["station_name"], conn)
                insert_measurements(all_data, conn)

            print(f"OK: {path} → {len(all_data)} entries")

        print("Done!")

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL:", _ex)
