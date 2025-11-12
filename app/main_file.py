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

    data_dic = {"host": None, "port": 5432,
                "user": None, "password": None, "database": None}

    with open(path, "r", encoding="utf-8", errors="replace") as f:

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

# !!! Enter personal password in config.env !!!


@lru_cache(maxsize=1)
def connect_engine(config_path: str = "app/config.env", *, echo: bool = False) -> Engine:

    cfg = load_db_config(config_path)
    db_url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    print("DB URL:", db_url)
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

# --------Find hours/stations in tables*.csv---------


def parse_sheet(df_raw, station_name):
    """
    Return:
      long_df:     station_name | date | hour(1..24) | db_level
      hours:       list of hours (exp., [1..24])
      daynight_df: station_name | date | laeq_day | laeq_night  
    """

    df = df_raw.copy().reset_index(drop=True)

    # 1) find if exist "측정일"
    header_row = None
    for i in range(min(30, len(df))):
        if "측정일" in " ".join(df.iloc[i].astype(str).tolist()):
            header_row = i
            break
    if header_row is None:
        empty_long = pd.DataFrame(
            columns=["station_name", "date", "hour", "db_level"])
        empty_dn = pd.DataFrame(
            columns=["station_name", "date", "laeq_day", "laeq_night"])
        return empty_long, [], empty_dn

    # 2) titles
    df.columns = df.iloc[header_row]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    # 3) date colomns  ("측정" or "시간")
    date_col = next((c for c in df.columns
                     if isinstance(c, str) and ("측정" in c or "시간" in c)), df.columns[0])

    # 4) date normalization
    dts = pd.to_datetime(df[date_col], errors="coerce")
    df = df[dts.notna()].copy()
    df["date"] = dts.dt.date

    # 5) group hour columns 1..24
    hour_cols, hours = [], []
    for c in df.columns:
        if c in (date_col, "date"):
            continue
        try:
            h = int(round(float(str(c).strip())))
            if 1 <= h <= 24:
                hour_cols.append(c)
                hours.append(h)
        except Exception:
            pass
    hours = sorted(set(hours))
    # --- wide -> long ---
    if hour_cols:
        long_df = df.melt(id_vars=["date"], value_vars=hour_cols,
                          var_name="hour_raw", value_name="db_level")
        long_df["hour"] = pd.to_numeric(
            long_df["hour_raw"], errors="coerce").round().astype("Int64")
        long_df.drop(columns=["hour_raw"], inplace=True)
        long_df["db_level"] = pd.to_numeric(
            long_df["db_level"].astype(str).str.replace(",", "."), errors="coerce"
        ).round(2)
        long_df = long_df.dropna(subset=["hour", "db_level"])
        long_df["station_name"] = station_name
        long_df["hour"] = long_df["hour"].astype(int)
        long_df = long_df[["station_name", "date", "hour", "db_level"]]
    else:
        long_df = pd.DataFrame(
            columns=["station_name", "date", "hour", "db_level"])

    # 6) day/night LAeq
    day_candidates = ["낮", "day", "Day", "DAY"]
    night_candidates = ["밤", "night", "Night", "NIGHT"]

    day_col = next((c for c in day_candidates if c in df.columns), None)
    night_col = next((c for c in night_candidates if c in df.columns), None)

    if day_col and night_col:
        dn = df[[date_col, day_col, night_col]].copy()
        dn = dn.rename(columns={date_col: "date",
                       day_col: "laeq_day", night_col: "laeq_night"})

        for c in ("laeq_day", "laeq_night"):
            dn[c] = pd.to_numeric(dn[c].astype(str).str.replace(
                ",", "."), errors="coerce").round(2)
        dn = dn.dropna(subset=["laeq_day", "laeq_night"])
        dn["station_name"] = station_name
        daynight_df = dn[["station_name", "date", "laeq_day", "laeq_night"]]
    else:
        daynight_df = pd.DataFrame(
            columns=["station_name", "date", "laeq_day", "laeq_night"])

    return long_df, hours, daynight_df

# ------ Create Database ------


def ensure_database(conn):
    conn.execute(text("CREATE DATABASE IF NOT EXISTS noise_db;"))

# ------ Create tables -------


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
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS noise_level_h (
            station_id   INT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
            ts_hour_kst  TIMESTAMP NOT NULL,
            n_samples    INT,
            laeq         NUMERIC(6,2) NOT NULL,
            created_at   TIMESTAMP DEFAULT now(),
            updated_at   TIMESTAMP,
            PRIMARY KEY (station_id, ts_hour_kst)
        );
    """))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_h_station_ts  ON noise_level_h (station_id, ts_hour_kst);"))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_r_station_ts  ON noise_reading (station_id, ts_utc);"))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_noise_ts      ON noise_reading (ts_utc);"))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_noise_station ON noise_reading (station_id);"))

# ------- Insert data in table "stations" ----------


def upsert_stations(names, conn):
    for n in sorted(set(names)):
        conn.execute(text("""
            INSERT INTO stations(name, geom)
            VALUES (:n, ST_SetSRID(ST_MakePoint(126.9780, 37.5665), 4326))
            ON CONFLICT (name) DO NOTHING;
        """), {"n": n})

# ------- Insert data in table "noise_reading" -----


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

# ------- Insert data in table "noise_level_l" -----


def refresh_hours_from_readings(conn, from_utc=None, to_utc=None):
    sql = text("""
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
    """)
    conn.execute(sql, {"from_utc": from_utc, "to_utc": to_utc})

# ------- Insert data in table "noise_level_d" -----


def insert_day_night_levels(all_dn, conn):
    df_tmp = all_dn[["station_name", "d_kst", "laeq_day", "laeq_night"]].dropna(
        subset=["laeq_day", "laeq_night"]).copy()

    conn.execute(text("DROP TABLE IF EXISTS _noise_day_tmp"))
    conn.execute(text("""
        CREATE TEMP TABLE _noise_day_tmp (
            station_name TEXT,
            d_kst DATE,
            laeq_day NUMERIC(6,2),
            laeq_night NUMERIC(6,2)
        ) ON COMMIT DROP;
    """))

    df_tmp.to_sql("_noise_day_tmp", con=conn, if_exists="append", index=False)

    conn.execute(text("""
        INSERT INTO noise_level_d (station_id, d_kst, laeq_day, laeq_night, created_at, updated_at)
        SELECT s.station_id, t.d_kst, t.laeq_day, t.laeq_night, now(), now()
        FROM _noise_day_tmp t
        JOIN stations s ON s.name = t.station_name
        ON CONFLICT (station_id, d_kst) DO UPDATE
        SET laeq_day   = EXCLUDED.laeq_day,
            laeq_night = EXCLUDED.laeq_night,
            updated_at = now();
    """))

# ------- Insert data in table "noise_level_h" -----


def insert_hours_levels(h_level, conn):
    df_tmp = (
        h_level[["station_name", "d_kst", "hour", "laeq"]]
        .dropna(subset=["d_kst", "hour", "laeq"])
        .copy()
    )

    conn.execute(text("DROP TABLE IF EXISTS _h_levels_tmp;"))
    conn.execute(text("""
        CREATE TEMP TABLE _h_levels_tmp(
          station_name TEXT,
          d_kst        DATE,
          hour         INT,
          laeq         NUMERIC(6,2)
        ) ON COMMIT DROP;
    """))

    df_tmp.to_sql("_h_levels_tmp", con=conn, if_exists="append", index=False)

    conn.execute(text("""
        INSERT INTO noise_level_h (station_id, ts_hour_kst, laeq, created_at, updated_at)
        SELECT
            s.station_id,
            (t.d_kst + (t.hour-1) * INTERVAL '1 hour')::timestamp AS ts_hour_kst,
            t.laeq,
            now(), now()
        FROM _h_levels_tmp t
        JOIN stations s ON s.name = t.station_name
        ON CONFLICT (station_id, ts_hour_kst) DO UPDATE
        SET laeq = EXCLUDED.laeq,
            updated_at= now();
    """))

# ------- Calculate noise peak time ----------------


def fetch_peak_times(conn, station_id=None, date_from=None, date_to=None):
    params = {"sid": station_id, "dfrom": date_from, "dto": date_to}
    q = text("""
        WITH hh AS (
                    SELECT station_id,
                            ts_hour_kst::date                   AS d_kst,
                            EXTRACT(HOUR FROM ts_hour_kst)::int AS h_kst,
                            laeq
                    FROM noise_level_h
                    WHERE (:sid   IS NULL OR station_id = :sid)
                        AND (:dfrom IS NULL OR ts_hour_kst::date >= :dfrom)
                        AND (:dto   IS NULL OR ts_hour_kst::date <  :dto)
                    ),
        day_peak AS (
                    SELECT DISTINCT ON (station_id, d_kst)
                            station_id, d_kst, h_kst, laeq
                    FROM hh
                    ORDER BY station_id, d_kst, laeq DESC, h_kst ASC
                    ),
        global_peak AS (
                    SELECT DISTINCT ON (station_id)
                            station_id, d_kst, h_kst, laeq
                    FROM hh
                    ORDER BY station_id, laeq DESC, d_kst ASC, h_kst ASC
                    )
        SELECT 'day_peak' AS kind, station_id, d_kst, h_kst AS hour_kst, laeq FROM day_peak
        UNION ALL
        SELECT 'global_peak', station_id, d_kst, h_kst, laeq FROM global_peak
        ORDER BY station_id, kind, d_kst NULLS LAST, hour_kst NULLS LAST
    """)
    df = pd.read_sql(q, conn, params=params)
    return (
        df[df["kind"] == "day_peak"].drop(
            columns=["kind"]).reset_index(drop=True),
        df[df["kind"] == "global_peak"].drop(
            columns=["kind"]).reset_index(drop=True),
    )

# ------------- main -------------


def main():
    try:
        engine = connect_engine()
        with engine.begin() as conn:
            ensure_database(conn)
            ensure_tables(conn)

        RAW_DIR = Path("data/raw")
        files = [p for p in RAW_DIR.glob(
            "*.xlsx") if not p.name.startswith("~$")]

        for path in files:
            print("→", path)

            # 1) Read sheets
            xls = pd.ExcelFile(path)
            sheets = {name: xls.parse(name, header=None)
                      for name in xls.sheet_names}

            # 2) parse
            frames_h, frames_dn = [], []
            for sheet_name, df in sheets.items():

                long_df, hours, daynight_df = parse_sheet(df, sheet_name)
                print(
                    f"  - {sheet_name}: parsed hours={len(long_df)} rows; day/night={len(daynight_df)} rows")
                if not long_df.empty:
                    frames_h.append(long_df)
                if daynight_df is not None and not daynight_df.empty:

                    frames_dn.append(daynight_df)

            # 3) if not exists -> pass
            total_h = sum(len(x) for x in frames_h)
            total_dn = sum(len(x) for x in frames_dn)
            if total_h == 0 and total_dn == 0:
                print(f"SKIP (no data): {path}")
                continue

            # 4) join
            all_hours = pd.concat(frames_h, ignore_index=True) if frames_h else pd.DataFrame(
                columns=["station_name", "date", "hour", "db_level"])
            all_dn = pd.concat(frames_dn, ignore_index=True) if frames_dn else pd.DataFrame(
                columns=["station_name", "d_kst", "laeq_day", "laeq_night"])

            # 5) clean station`s name
            if not all_hours.empty:
                all_hours["station_name"] = (
                    all_hours["station_name"].astype(str)
                    .str.replace(r"\(시간별\)", "", regex=True)
                    .str.strip()
                )
            if not all_dn.empty:
                all_dn["station_name"] = (
                    all_dn["station_name"].astype(str)
                    .str.replace(r"\(시간별\)", "", regex=True)
                    .str.strip()
                )

            # 6) insert into database
            with engine.begin() as conn:

                names = pd.concat([
                    all_hours["station_name"]] + ([all_dn["station_name"]] if not all_dn.empty else []),
                    ignore_index=True
                ) if not all_hours.empty or not all_dn.empty else pd.Series(dtype=str)
                if not names.empty:
                    upsert_stations(names, conn)

                if not all_hours.empty:
                    insert_measurements(all_hours, conn)

                if not all_hours.empty:
                    refresh_hours_from_readings(conn)

                if not all_dn.empty:
                    if "date" in all_dn.columns:
                        all_dn = all_dn.rename(columns={"date": "d_kst"})
                    insert_day_night_levels(all_dn, conn)

            print(
                f"OK: {path.name} → hours:{len(all_hours)}  day/night:{len(all_dn)}")

        print("Done!")

    except Exception as _ex:
        import traceback
        traceback.print_exc()
        print("[INFO] Error while working with PostgreSQL:", _ex)


if __name__ == "__main__":
    main()
